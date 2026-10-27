#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <cerrno>
#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <csignal>

// ---------- shared by all worker threads ----------
int backend_ports[] = {9001, 9002, 9003, 9004, 9005};
const int num_backends = 5;
std::string mode = "rr";                     // rr | lc | hash
bool verbose = false;                        // per-connection logs (set LB_VERBOSE=1)

std::map<uint32_t, int> ring;                // position on the clock → backend index (read-only after startup)
std::atomic<unsigned> next_backend{0};       // round-robin turn, shared so the rotation is global
std::atomic<int>  active_conns[num_backends];
std::atomic<bool> healthy[num_backends];
std::atomic<bool> draining[num_backends];
std::atomic<int>  fail_count[num_backends];
std::atomic<int>  ok_count[num_backends];

// ---------- private to each worker thread ----------
thread_local std::unordered_map<int, int> partner;            // fd → its partner fd
thread_local std::unordered_map<int, std::string> pending;    // bytes waiting to be written TO this fd
thread_local std::unordered_set<int> connecting;              // backend fds still dialling
thread_local std::unordered_map<int, int> backend_of;         // backend fd → backend index
thread_local std::unordered_map<int, uint32_t> client_ip_of;  // client fd → client IP
thread_local std::unordered_map<int, int> probe_of;           // health-check fd → backend index (worker 0 only)

void build_ring() {
    for (int b = 0; b < num_backends; b++) {
        for (int v = 0; v < 100; v++) {
            std::string key = std::to_string(backend_ports[b]) + "#" + std::to_string(v);
            uint32_t pos = std::hash<std::string>{}(key);
            ring[pos] = b;
        }
    }
}

// a backend can take NEW clients only if it is healthy and not draining
bool usable(int b) { return healthy[b] && !draining[b]; }

int pick_round_robin() {
    for (int tries = 0; tries < num_backends; tries++) {
        int idx = next_backend.fetch_add(1) % num_backends;
        if (usable(idx)) return idx;
    }
    return -1;
}

int pick_least_conn() {
    int best = -1;
    for (int b = 0; b < num_backends; b++) {
        if (!usable(b)) continue;
        if (best == -1 || active_conns[b] < active_conns[best]) best = b;
    }
    return best;
}

int pick_hash(uint32_t client_ip) {
    uint32_t pos = std::hash<std::string>{}(std::to_string(client_ip));
    auto it = ring.lower_bound(pos);
    for (size_t k = 0; k < ring.size(); k++) {
        if (it == ring.end()) it = ring.begin();
        if (usable(it->second)) return it->second;
        ++it;
    }
    return -1;
}

// active health check result: 3 fails in a row → DOWN, 2 successes in a row → UP
void record_result(int b, bool ok) {
    if (ok) {
        fail_count[b] = 0;
        ok_count[b]++;
        if (ok_count[b] >= 2 && !healthy[b].exchange(true))
            printf("backend %d UP\n", backend_ports[b]);
    } else {
        ok_count[b] = 0;
        fail_count[b]++;
        if (fail_count[b] >= 3 && healthy[b].exchange(false))
            printf("backend %d DOWN\n", backend_ports[b]);
    }
}

// passive health check: real traffic failed to connect → DOWN right away
void mark_down(int b) {
    if (!healthy[b].exchange(false)) return;     // already DOWN
    fail_count[b] = 3;
    ok_count[b] = 0;
    printf("backend %d DOWN (connect failed)\n", backend_ports[b]);
}

// one client left backend b
void release_backend(int b) {
    int left = --active_conns[b];
    if (draining[b] && left == 0)
        printf("backend %d fully drained: safe to stop\n", backend_ports[b]);
}

void close_pair(int fd) {
    int other = partner[fd];                     // look up the partner BEFORE erasing
    if (backend_of.count(fd))    { release_backend(backend_of[fd]);    backend_of.erase(fd); }
    if (backend_of.count(other)) { release_backend(backend_of[other]); backend_of.erase(other); }
    if (verbose) printf("closing %d and %d\n", fd, other);
    close(fd);
    close(other);
    partner.erase(fd);
    partner.erase(other);
    pending.erase(fd);
    pending.erase(other);
    connecting.erase(fd);
    connecting.erase(other);
    client_ip_of.erase(fd);
    client_ip_of.erase(other);
}

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void watch(int epfd, int fd, uint32_t events) {
    epoll_event e{};
    e.events = events;
    e.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}

bool has_backlog(int fd) {
    auto it = pending.find(fd);
    return it != pending.end() && !it->second.empty();
}

// set fd's watch from its state: read only while its partner has no backlog,
// ask for EPOLLOUT only while fd itself has a backlog
void refresh(int epfd, int fd) {
    uint32_t ev = 0;
    if (!has_backlog(partner[fd])) ev |= EPOLLIN;
    if (has_backlog(fd))           ev |= EPOLLOUT;
    watch(epfd, fd, ev);
}

// pick a backend and start dialling it; returns false if it couldn't
bool connect_backend(int epfd, int client_fd) {
    int idx;
    if (mode == "lc")        idx = pick_least_conn();
    else if (mode == "hash") idx = pick_hash(client_ip_of[client_fd]);
    else                     idx = pick_round_robin();
    if (idx < 0) { printf("no healthy backends!\n"); return false; }

    int backend_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (backend_fd < 0) return false;
    set_nonblocking(backend_fd);

    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(backend_ports[idx]);
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);

    if (connect(backend_fd, (struct sockaddr *)&a, sizeof(a)) < 0 && errno != EINPROGRESS) {
        mark_down(idx);                          // refused instantly → don't pick it again
        close(backend_fd);
        return false;
    }

    epoll_event bev{};
    bev.events = EPOLLOUT;                       // tell me when the dialling finishes
    bev.data.fd = backend_fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, backend_fd, &bev) < 0) { close(backend_fd); return false; }

    partner[client_fd] = backend_fd;
    partner[backend_fd] = client_fd;
    connecting.insert(backend_fd);
    backend_of[backend_fd] = idx;
    active_conns[idx]++;
    if (verbose) printf("client %d -> backend %d\n", client_fd, backend_ports[idx]);
    return true;
}

bool connect_with_retry(int epfd, int client_fd) {
    for (int attempt = 0; attempt < 3; attempt++) {
        if (connect_backend(epfd, client_fd)) return true;
    }
    return false;
}

// on every timer tick: count unfinished probes as failures, then probe every backend again
void start_probes(int epfd) {
    for (auto &p : probe_of) {
        record_result(p.second, false);
        close(p.first);
    }
    probe_of.clear();

    for (int b = 0; b < num_backends; b++) {
        int pfd = socket(AF_INET, SOCK_STREAM, 0);
        if (pfd < 0) continue;
        set_nonblocking(pfd);

        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port = htons(backend_ports[b]);
        inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);

        int rc = connect(pfd, (struct sockaddr *)&a, sizeof(a));
        if (rc == 0)              { record_result(b, true);  close(pfd); continue; }  // connected instantly
        if (errno != EINPROGRESS) { record_result(b, false); close(pfd); continue; }  // refused instantly

        epoll_event pe{};
        pe.events = EPOLLOUT;
        pe.data.fd = pfd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, pfd, &pe);
        probe_of[pfd] = b;
    }
}

// typed commands: "drain <port>" / "undrain <port>"
void handle_command(int epfd) {
    char line[64];
    ssize_t r = read(STDIN_FILENO, line, sizeof(line) - 1);
    if (r <= 0) { epoll_ctl(epfd, EPOLL_CTL_DEL, STDIN_FILENO, nullptr); return; }  // no keyboard → stop watching
    line[r] = '\0';

    int port;
    bool on;
    if (sscanf(line, "drain %d", &port) == 1)        on = true;
    else if (sscanf(line, "undrain %d", &port) == 1) on = false;
    else { printf("commands: drain <port> | undrain <port>\n"); return; }

    for (int b = 0; b < num_backends; b++) {
        if (backend_ports[b] != port) continue;
        draining[b] = on;
        printf("backend %d %s (%d active)\n", port, on ? "DRAINING" : "back in service", active_conns[b].load());
        if (on && active_conns[b] == 0)
            printf("backend %d fully drained: safe to stop\n", port);
    }
}

// one event loop per thread; each thread has its own listening socket on 8080 (SO_REUSEPORT)
void worker(int id) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(1); }
    set_nonblocking(server_fd);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        perror("setsockopt(SO_REUSEPORT)");
        exit(1);
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(8080);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) { perror("bind"); exit(1); }
    if (listen(server_fd, SOMAXCONN) < 0) { perror("listen"); exit(1); }

    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); exit(1); }
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    // only worker 0 runs the health checks and reads typed commands
    int tfd = -1;
    if (id == 0) {
        tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
        if (tfd < 0) { perror("timerfd_create"); exit(1); }
        itimerspec spec{};
        spec.it_value.tv_sec = 1;                // first ring after 1 second
        spec.it_interval.tv_sec = 1;             // then every 1 second
        timerfd_settime(tfd, 0, &spec, nullptr);
        epoll_event tev{};
        tev.events = EPOLLIN;
        tev.data.fd = tfd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, tfd, &tev);

        epoll_event sev{};
        sev.events = EPOLLIN;
        sev.data.fd = STDIN_FILENO;
        epoll_ctl(epfd, EPOLL_CTL_ADD, STDIN_FILENO, &sev);
    }

    epoll_event events[64];
    while (true) {
        int n = epoll_wait(epfd, events, 64, -1);
        if (n < 0) { if (errno != EINTR) perror("epoll_wait"); continue; }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            // ⏰ alarm clock: start this round of health checks
            if (fd == tfd) {
                uint64_t ticks;
                if (read(tfd, &ticks, sizeof(ticks)) < 0) continue;   // clear the alarm
                start_probes(epfd);
                continue;
            }

            // ⌨️ a typed command
            if (id == 0 && fd == STDIN_FILENO) {
                handle_command(epfd);
                continue;
            }

            // 🧪 a health-check call finished
            if (probe_of.count(fd)) {
                int err = 0;
                socklen_t len = sizeof(err);
                getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
                record_result(probe_of[fd], err == 0);
                probe_of.erase(fd);
                close(fd);
                continue;
            }

            // 🚪 door: a new client
            if (fd == server_fd) {
                sockaddr_in client_addr{};
                socklen_t addr_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
                if (client_fd < 0) continue;     // EAGAIN: another thread got it first
                set_nonblocking(client_fd);

                epoll_event cev{};
                cev.events = 0;                  // don't listen until the backend is connected
                cev.data.fd = client_fd;
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &cev) < 0) { close(client_fd); continue; }

                client_ip_of[client_fd] = client_addr.sin_addr.s_addr;
                if (!connect_with_retry(epfd, client_fd)) {
                    client_ip_of.erase(client_fd);
                    close(client_fd);
                }
                continue;
            }

            // 📞 a client or backend phone
            if (partner.find(fd) == partner.end()) continue;     // already closed earlier in this batch

            // the backend dialling finished
            if (connecting.count(fd)) {
                int err = 0;
                socklen_t len = sizeof(err);
                getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
                int client = partner[fd];
                connecting.erase(fd);

                if (err == 0) {
                    watch(epfd, fd, EPOLLIN);           // backend: normal
                    watch(epfd, client, EPOLLIN);       // client: start listening now
                    continue;
                }

                // dial failed: mark the backend DOWN now, hang up only the backend side, try another
                int idx = backend_of[fd];
                if (verbose) printf("backend %d connect failed: %s\n", backend_ports[idx], strerror(err));
                mark_down(idx);
                release_backend(idx);
                backend_of.erase(fd);
                partner.erase(fd);
                pending.erase(fd);
                close(fd);

                if (connect_with_retry(epfd, client)) continue;

                // every attempt failed; partner[client] still names the closed fd, so no close_pair here
                printf("client %d dropped: no backend reachable\n", client);
                partner.erase(client);
                pending.erase(client);
                client_ip_of.erase(client);
                close(client);
                continue;
            }

            // there's room again → deliver the sticky note
            uint32_t evs = events[i].events;
            if (evs & EPOLLOUT) {
                std::string &note = pending[fd];
                ssize_t w = write(fd, note.data(), note.size());
                if (w < 0 && errno != EAGAIN && errno != EWOULDBLOCK) { close_pair(fd); continue; }
                if (w > 0) note.erase(0, w);
                if (note.empty()) {
                    refresh(epfd, fd);                  // stop asking for EPOLLOUT
                    refresh(epfd, partner[fd]);         // un-pause the talker
                }
                continue;
            }

            // someone talked → pass it on
            char buf[16384];
            ssize_t r = read(fd, buf, sizeof(buf));
            int other = partner[fd];
            if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;   // nothing right now
            if (r <= 0) { close_pair(fd); continue; }                         // hung up or error

            if (has_backlog(other)) {                    // older bytes still waiting: keep the order
                pending[other].append(buf, r);
                continue;
            }
            ssize_t w = write(other, buf, r);
            if (w < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) w = 0;           // busy, not broken
                else { close_pair(fd); continue; }
            }
            if (w < r) {
                pending[other].append(buf + w, r - w);   // save what wasn't sent
                refresh(epfd, other);                    // tell me when other has room
                refresh(epfd, fd);                       // pause the talker (backpressure)
            }
        }
    }
}

int main(int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN);    // writing to a closed socket must return an error, not kill us
    signal(SIGTTIN, SIG_IGN);    // reading stdin while in the background must not stop us
    setvbuf(stdout, nullptr, _IOLBF, 0);   // print each log line immediately, even into a file

    if (argc > 1) mode = argv[1];
    int threads = (argc > 2) ? atoi(argv[2]) : (int)std::thread::hardware_concurrency();
    if (threads < 1) threads = 1;
    verbose = getenv("LB_VERBOSE") != nullptr;

    for (int b = 0; b < num_backends; b++) {
        active_conns[b] = 0;
        healthy[b] = true;
        draining[b] = false;
        fail_count[b] = 0;
        ok_count[b] = 0;
    }
    build_ring();

    printf("load balancing mode: %s, %d worker threads\n", mode.c_str(), threads);
    fflush(stdout);

    std::vector<std::thread> pool;
    for (int t = 0; t < threads; t++) pool.emplace_back(worker, t);
    for (auto &t : pool) t.join();
}

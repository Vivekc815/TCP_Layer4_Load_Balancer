#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstdio>
#include <sys/epoll.h>
#include <unordered_map>
#include <fcntl.h>
#include <cerrno>
#include <string>
#include <unordered_set>
#include <cstring>
#include <map>
#include <functional>


char buffer[1024];
std::unordered_set<int> connecting; 
std::unordered_map<int, int> partner;   // fd → its partner fdho
std::unordered_map<int, std::string> pending;
std::map<uint32_t, int> ring;            // position on the clock → backend index
int backend_ports[] = {9001, 9002, 9003, 9004, 9005};
int num_backends = 5;
int next_backend = 0; 
int active_conns[5] = {0};  
std::unordered_map<int, int> backend_of;   


void build_ring(){
    for(int b=0; b<num_backends; b++){
        for(int v = 0; v<100; v++){
            std::string key = std::to_string(backend_ports[b]) + "#" + std::to_string(v);
            uint32_t pos = std::hash<std::string>{}(key);
            ring[pos] = b;
        }
    }
}

int pick_hash(uint32_t client_ip){
    uint32_t pos = std::hash<std::string>{}(std::to_string(client_ip));
    auto it = ring.lower_bound(pos);
    if(it == ring.end()) it = ring.begin();
    return it-> second;
}

void close_pair(int fd) {
    if (backend_of.count(fd))    { active_conns[backend_of[fd]]--;    backend_of.erase(fd); }
    int other = partner[fd]; 
    if (backend_of.count(other)) { active_conns[backend_of[other]]--; backend_of.erase(other); }
               // look up the partner BEFORE erasing
    printf("closing %d and %d\n", fd, other);
    close(fd);
    close(other);
    partner.erase(fd);
    partner.erase(other);
    pending.erase(fd);
    pending.erase(other);
    connecting.erase(fd);
    connecting.erase(other);
}


void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void watch(int epfd, int fd, uint32_t events){
    epoll_event e{};
    e.events = events;
    e.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}

int pick_round_robin(){
    int idx = next_backend;
    next_backend = (next_backend + 1) % num_backends;
    return idx;
}

int pick_least_conn(){
    int best = 0;
    for(int b = 1; b< num_backends; b++){
        if(active_conns[b]< active_conns[best]){
            best = b;
        }
        
    }
    return best;
}

int main(int argc, char* argv[]){
    std::string mode = "rr";
    if (argc > 1) mode = argv[1];
    printf("load balancing mode: %s\n", mode.c_str());


    
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }
    set_nonblocking(server_fd);


    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(8080);
    address.sin_addr.s_addr = htonl(INADDR_ANY);

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
        return 1;
    }

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
            perror("bind");
            return 1;
    }    
    if (listen(server_fd, 10) < 0) {
        perror("listen");
        return 1;
    }
 
    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); return 1; }
    printf("epoll created, fd = %d\n", epfd);
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    
    if(epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev)<0){
        perror("epoll_ctl");
        return 1;
    }
    printf("server_fd added to epoll\n");

    epoll_event events[64];
    build_ring();
    
    while (true){
        int n = epoll_wait(epfd, events, 64, -1);
        if (n<0){ perror("epoll_wait"); continue;}

        for(int i=0; i<n; i++){
            int fd = events[i].data.fd;

            if(fd == server_fd){
                sockaddr_in client_addr{};
                socklen_t addr_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
                if (client_fd < 0) { perror("accept"); continue; }
                set_nonblocking(client_fd);
                printf("new client: fd %d\n", client_fd);


                epoll_event cev{};
                cev.events = 0;
                cev.data.fd =client_fd;
                if(epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &cev)<0){
                    perror("epoll_ctl");
                    close(client_fd);
                    continue;
                }
                int backend_fd = socket(AF_INET, SOCK_STREAM, 0);
                if (backend_fd < 0) { perror("socket"); close(client_fd); continue; }
                int idx;
                if (mode == "lc")        idx = pick_least_conn();
                else if (mode == "hash") idx = pick_hash(client_addr.sin_addr.s_addr);
                else                     idx = pick_round_robin();
                int port = backend_ports[idx];
                printf("client %d -> backend %d (port %d)\n", client_fd, idx, port);
                sockaddr_in backend_addr{};
                backend_addr.sin_family = AF_INET;
                backend_addr.sin_port   = htons(port);
                inet_pton(AF_INET, "127.0.0.1", &backend_addr.sin_addr);
                set_nonblocking(backend_fd);
                if (connect(backend_fd, (struct sockaddr *)&backend_addr, sizeof(backend_addr)) < 0 && errno != EINPROGRESS) {
                    perror("connect");
                    close(backend_fd);
                    close(client_fd);
                    continue;
                }
                


                epoll_event bev{};
                bev.events = EPOLLOUT;
                bev.data.fd = backend_fd;
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, backend_fd, &bev) < 0) {
                    perror("epoll_ctl");
                    close(backend_fd);
                    close(client_fd);
                    continue;
                }

                partner[client_fd] = backend_fd;
                partner[backend_fd] = client_fd;
                connecting.insert(backend_fd);
                backend_of[backend_fd] = idx;
                active_conns[idx]++;
                printf("paired client %d <-> backend %d\n", client_fd, backend_fd);
            } 
            
            else{
                if (partner.find(fd) == partner.end()) continue;
                // EXISTING CLIENT SENT DAT
                if (connecting.count(fd)) {                        // this fd was dialling
                    int err = 0;
                    socklen_t len = sizeof(err);
                    getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);  // did the call succeed?
                    int other = partner[fd];
                    connecting.erase(fd);

                    if (err != 0) {
                        printf("backend connect failed: %s\n", strerror(err));
                        // close both + erase from partner and pending
                        close_pair(fd);
                        continue;                    
                    }
                    watch(epfd, fd, EPOLLIN);       // backend: normal
                    watch(epfd, other, EPOLLIN);    // client: start listening now
                    continue;
                }

                uint32_t evs = events[i].events;
                if(evs & EPOLLOUT){
                    std::string &note = pending[fd];
                    ssize_t w = write(fd, note.data(), note.size());   
                    if (w < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    close_pair(fd);
                    continue;
                }
                if (w > 0) note.erase(0, w);
                if(note.empty()){
                    watch(epfd, fd, EPOLLIN);
                    watch(epfd, partner[fd], EPOLLIN);

                }
                continue;
            }

                char buf[1024];
                ssize_t r = read(fd, buf, sizeof(buf));
                int other = partner[fd];                
                if (r <= 0) {
                    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;   // nothing to read right now → skipnj
                    // 0 = client disconnected, -1 = error → clean up
                    printf("fd %d closed, closing partner %d\n", fd, other);
                    close_pair(fd);
                    continue;
                } else {
                    ssize_t w = write(other, buf, r);
                    if(w<0){
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        w = 0;    

                    }  
                    else{
                        close_pair(fd);
                        continue;

                    }

                }
                if(w<r){
                    pending[other].append(buf+w, r-w);
                    watch(epfd, other, EPOLLIN | EPOLLOUT);
                    watch(epfd, fd, 0);
                }
            }

        
        }
    }
    }

}
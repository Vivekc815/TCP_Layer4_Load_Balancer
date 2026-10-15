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


char buffer[1024];
std::unordered_set<int> connecting; 

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

int main(){
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
    std::unordered_map<int, int> partner;   // fd → its partner fdho
    std::unordered_map<int, std::string> pending;
    while (true){
        int n = epoll_wait(epfd, events, 64, -1);
        if (n<0){ perror("epoll_wait"); continue;}

        for(int i=0; i<n; i++){
            int fd = events[i].data.fd;

            if(fd == server_fd){
                int client_fd = accept(server_fd, nullptr, nullptr);
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
                sockaddr_in backend_addr{};
                backend_addr.sin_family = AF_INET;
                backend_addr.sin_port   = htons(9000);
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
                        close(fd);
                    close(other);
                    partner.erase(fd);            // remove both from the map
                    partner.erase(other);
                    pending.erase(fd);
                    pending.erase(other);
                    
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
                    int other = partner[fd];  
                    close(fd);
                    close(other);
                    partner.erase(fd);            // remove both from the map
                    partner.erase(other);
                    pending.erase(fd);
                    pending.erase(other);
                    connecting.erase(fd);
connecting.erase(other);
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
                    close(fd);
                    close(other);
                    partner.erase(fd);            // remove both from the map
                    partner.erase(other);
                    pending.erase(fd);
                    pending.erase(other);
                    connecting.erase(fd);
connecting.erase(other);
                } else {
                    ssize_t w = write(other, buf, r);
                    if(w<0){
                         if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        w = 0;    

                    }  
                    else{
                        close(fd);
                        close(other);
                        partner.erase(fd);            // remove both from the map
                        partner.erase(other);
                        pending.erase(fd);            // remove both from the map
                        pending.erase(other);
                        connecting.erase(fd);
connecting.erase(other);
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
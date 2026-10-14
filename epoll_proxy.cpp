#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstdio>
#include <sys/epoll.h>
#include <unordered_map>
#include <fcntl.h>
#include <cerrno>




char buffer[1024];

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
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
                cev.events = EPOLLIN;
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
                                if (connect(backend_fd, (struct sockaddr *)&backend_addr, sizeof(backend_addr)) < 0) {
                    perror("connect");
                    close(backend_fd);
                    close(client_fd);
                    continue;
                }
                set_nonblocking(backend_fd);


                epoll_event bev{};
                bev.events = EPOLLIN;
                bev.data.fd = backend_fd;
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, backend_fd, &bev) < 0) {
                    perror("epoll_ctl");
                    close(backend_fd);
                    close(client_fd);
                    continue;
                }

                partner[client_fd] = backend_fd;
                partner[backend_fd] = client_fd;
                printf("paired client %d <-> backend %d\n", client_fd, backend_fd);
            } 
            
            else{
                if (partner.find(fd) == partner.end()) continue;
                // EXISTING CLIENT SENT DATA
                char buf[1024];
                ssize_t r = read(fd, buf, sizeof(buf));
                int other = partner[fd];
                if (r <= 0) {
                    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;   // nothing to read right now → skip
                    // 0 = client disconnected, -1 = error → clean up
                    printf("fd %d closed, closing partner %d\n", fd, other);
                    close(fd);
                    close(other);
                    partner.erase(fd);            // remove both from the map
                    partner.erase(other);
                } else {
                    write(other, buf, r);  
                }
            }

        
        }
    }


}
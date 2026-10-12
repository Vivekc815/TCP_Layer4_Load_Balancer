#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstdio>
#include <sys/epoll.h>




char buffer[1024];

int main(){
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }


    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(9000);
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
    while (true){
        int n = epoll_wait(epfd, events, 64, -1);
        if (n<0){ perror("epoll_wait"); continue;}

        for(int i=0; i<n; i++){
            int fd = events[i].data.fd;

            if(fd == server_fd){
                int client_fd = accept(server_fd, nullptr, nullptr);
                if (client_fd < 0) { perror("accept"); continue; }
                printf("new client: fd %d\n", client_fd);


                epoll_event cev{};
                cev.events = EPOLLIN;
                cev.data.fd =client_fd;
                if(epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &cev)<0){
                    perror("epoll_ctl");
                    close(client_fd);

                }
            } else{
                // EXISTING CLIENT SENT DATA
                char buf[1024];
                ssize_t r = read(fd, buf, sizeof(buf));

                if (r <= 0) {
                    // 0 = client disconnected, -1 = error → clean up
                    printf("client fd %d disconnected\n", fd);
                    close(fd);
                } else {
                    write(fd, buf, r);   // echo back
                }
            }
        }
    }


}
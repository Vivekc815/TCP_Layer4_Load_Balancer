#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstdio>
#include <thread>



char buffer[1024];



void pump(int from_fd, int to_fd) {
    char buffer1[1024];
    ssize_t l;
    // loop: read from from_fd, write to to_fd, stop when read returns <= 0
    while((l = read(from_fd, buffer1, sizeof(buffer1)))>0){
        write(to_fd, buffer1, l);
    }
    shutdown(to_fd, SHUT_WR);

}


int main(){
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }


    sockaddr_in listen_addr{};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(8080);
    listen_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
        return 1;
    }

    if (bind(server_fd, (struct sockaddr *)&listen_addr, sizeof(listen_addr)) < 0) {
            perror("bind");
            return 1;
    }    
    if (listen(server_fd, 10) < 0) {
        perror("listen");
        return 1;
    }


    int client_fd;
    ssize_t n, m;
    while(true){
        client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) {
            perror("accept");
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
        std::thread t(pump, client_fd, backend_fd);
        pump(backend_fd, client_fd);
        t.join();



        
        
         
        close(backend_fd);
        close(client_fd);
    }
}


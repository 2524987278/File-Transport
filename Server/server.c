#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <signal.h>

#define BUF_SIZE 8192
#define PORT 9000

/* 大小端转换 */
uint64_t htonll(uint64_t v) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return ((uint64_t)htonl((uint32_t)(v & 0xFFFFFFFFULL)) << 32) | htonl((uint32_t)(v >> 32));
#else
    return v;
#endif
}

uint64_t ntohll(uint64_t v) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return ((uint64_t)ntohl((uint32_t)(v & 0xFFFFFFFFULL)) << 32) | ntohl((uint32_t)(v >> 32));
#else
    return v;
#endif
}

/* 发送全部数据 */
ssize_t send_all(int sock, const void *buf, size_t len) {
    size_t total = 0;
    const char *p = buf;
    while (total < len) {
        ssize_t n = send(sock, p + total, len - total, 0);
        if (n <= 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        total += n;
    }
    return total;
}

/* 接收全部数据 */
ssize_t recv_all(int sock, void *buf, size_t len) {
    size_t total = 0;
    char *p = buf;
    while (total < len) {
        ssize_t n = recv(sock, p + total, len - total, 0);
        if (n <= 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        total += n;
    }
    return total;
}

/* 处理上传：从 offset 开始写，直到 filesize */
int handle_upload(int sock, const char *filename, uint64_t filesize, uint64_t offset) {
    FILE *fp = fopen(filename, "r+b");
    if (!fp) {
        fp = fopen(filename, "wb+");  // 不存在则创建
    }
    if (!fp) {
        perror("fopen");
        return -1;
    }

    int fd = fileno(fp);

    // 如果现有文件比 offset 大，先截断到 offset，避免旧数据残留
    struct stat st;
    if (fstat(fd, &st) == 0) {
        if ((uint64_t)st.st_size > offset) {
            if (ftruncate(fd, (off_t)offset) != 0) {
                perror("ftruncate");
                fclose(fp);
                return -1;
            }
        }
    }

    if (fseeko(fp, (off_t)offset, SEEK_SET) != 0) {
        perror("fseeko");
        fclose(fp);
        return -1;
    }

    char buf[BUF_SIZE];
    uint64_t received = offset;
    while (received < filesize) {
        size_t to_read = (size_t)((filesize - received) > BUF_SIZE ? BUF_SIZE : (filesize - received));
        ssize_t n = recv(sock, buf, to_read, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("recv");
            fclose(fp);
            return -1;
        }
        if (n == 0) {
            fprintf(stderr, "client closed during upload\n");
            fclose(fp);
            return -1;
        }
        if (fwrite(buf, 1, (size_t)n, fp) != (size_t)n) {
            perror("fwrite");
            fclose(fp);
            return -1;
        }
        received += (uint64_t)n;
    }

    fflush(fp);
    fsync(fd);
    fclose(fp);
    return 0;
}

/* 处理下载：按照 client_offset 协商 server_offset 并从该处开始发送 */
int handle_download(int sock, const char *filename, uint64_t client_offset) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        perror("fopen");
        return -1;
    }

    struct stat st;
    if (stat(filename, &st) != 0) {
        perror("stat");
        fclose(fp);
        return -1;
    }

    uint64_t filesize = (uint64_t)st.st_size;
    uint64_t server_offset = client_offset > filesize ? filesize : client_offset;

    uint64_t net_filesize = htonll(filesize);
    uint64_t net_server_offset = htonll(server_offset);

    if (send_all(sock, &net_filesize, sizeof(net_filesize)) != sizeof(net_filesize)) {
        perror("send filesize");
        fclose(fp);
        return -1;
    }
    if (send_all(sock, &net_server_offset, sizeof(net_server_offset)) != sizeof(net_server_offset)) {
        perror("send server_offset");
        fclose(fp);
        return -1;
    }

    if (server_offset >= filesize) {
        fclose(fp);
        return 0; // 对端已完整，无需发送
    }

    if (fseeko(fp, (off_t)server_offset, SEEK_SET) != 0) {
        perror("fseeko");
        fclose(fp);
        return -1;
    }

    char buf[BUF_SIZE];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        if (send_all(sock, buf, n) != (ssize_t)n) {
            perror("send");
            fclose(fp);
            return -1;
        }
    }
    if (ferror(fp)) {
        perror("fread");
        fclose(fp);
        return -1;
    }

    fclose(fp);
    return 0;
}

/* 客户端处理：与 client.c 协议匹配，并保证早退时关闭套接字 */
void handle_client(int client_sock) {
    uint32_t mode_len_net, filename_len_net;
    char mode[32], filename[512];

    // 统一用 goto cleanup，避免早退不 close
    if (recv_all(client_sock, &mode_len_net, sizeof(mode_len_net)) != sizeof(mode_len_net)) goto cleanup;
    uint32_t mode_len = ntohl(mode_len_net);
    if (mode_len == 0 || mode_len >= sizeof(mode)) goto cleanup;
    if (recv_all(client_sock, mode, mode_len) != (ssize_t)mode_len) goto cleanup;
    mode[mode_len] = '\0';

    if (recv_all(client_sock, &filename_len_net, sizeof(filename_len_net)) != sizeof(filename_len_net)) goto cleanup;
    uint32_t filename_len = ntohl(filename_len_net);
    if (filename_len == 0 || filename_len >= sizeof(filename)) goto cleanup;
    if (recv_all(client_sock, filename, filename_len) != (ssize_t)filename_len) goto cleanup;
    filename[filename_len] = '\0';

    if (strcmp(mode, "upload") == 0) {
        // 3) C->S: filesize
        uint64_t filesize_net;
        if (recv_all(client_sock, &filesize_net, sizeof(filesize_net)) != sizeof(filesize_net)) goto cleanup;
        uint64_t filesize = ntohll(filesize_net);

        // 4) S->C: agreed_offset（本地已有大小，裁剪到 filesize）
        uint64_t existing = 0;
        struct stat st;
        if (stat(filename, &st) == 0) {
            existing = (uint64_t)st.st_size;
        } else if (errno != ENOENT) {
            perror("stat");
            goto cleanup;
        }
        uint64_t agreed = existing > filesize ? filesize : existing;
        uint64_t net_agreed = htonll(agreed);
        if (send_all(client_sock, &net_agreed, sizeof(net_agreed)) != sizeof(net_agreed)) goto cleanup;

        // 5) 接收 [agreed, filesize) 的数据
        (void)handle_upload(client_sock, filename, filesize, agreed);
    }
    else if (strcmp(mode, "download") == 0) {
        // 3) C->S: client_offset
        uint64_t offset_net;
        if (recv_all(client_sock, &offset_net, sizeof(offset_net)) != sizeof(offset_net)) goto cleanup;
        uint64_t client_offset = ntohll(offset_net);

        // 4) S->C: filesize + server_offset, 然后发数据
        (void)handle_download(client_sock, filename, client_offset);
    }

cleanup:
    close(client_sock);
}

int main() {
    signal(SIGPIPE, SIG_IGN);  // 忽略 SIGPIPE，send 出错时只返回 -1，不会杀进程
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        perror("socket");
        exit(1);
    }

    int opt = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(1);
    }

    if (listen(server_sock, 5) < 0) {
        perror("listen");
        exit(1);
    }

    printf("Server listening on port %d...\n", PORT);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_sock = accept(server_sock, (struct sockaddr*)&client_addr, &client_len);
        if (client_sock < 0) {
            perror("accept");
            continue;
        }

        printf("Client connected: %s:%d\n",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        handle_client(client_sock);
    }

    close(server_sock);
    return 0;
}

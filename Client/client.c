/*
 * client.c
 * 改进后的文件传输客户端，支持断点续传（与 server.c 协议匹配）
 *
 * Usage: client upload|download <server_ip> <server_port> <filename>
 *
 * 协议（network byte order, no terminating NULs）:
 * 1) client -> server: uint32_t mode_len, mode bytes (mode_len)
 * 2) client -> server: uint32_t name_len, filename bytes (name_len)
 *
 * upload:
 * 3) client -> server: uint64_t filesize
 * 4) server -> client: uint64_t agreed_offset
 * 5) client -> server: file bytes starting from agreed_offset to EOF
 *
 * download:
 * 3) client -> server: uint64_t client_offset
 * 4) server -> client: uint64_t filesize, uint64_t server_offset
 * 5) server -> client: file bytes starting from server_offset to EOF
 *
 * 本地会生成 <filename>.progress 用于记录已发送/已接收字节数（写入是原子性的）
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <endian.h>
#include <time.h>

#define CHUNK 8192

/* htonll/ntohll: 大小端安全实现 */
static inline uint64_t htonll(uint64_t v) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return ((uint64_t)htonl((uint32_t)(v & 0xffffffffULL)) << 32) |
           (uint64_t)htonl((uint32_t)(v >> 32));
#else
    return v;
#endif
}
static inline uint64_t ntohll(uint64_t v) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
    return ((uint64_t)ntohl((uint32_t)(v & 0xffffffffULL)) << 32) |
           (uint64_t)ntohl((uint32_t)(v >> 32));
#else
    return v;
#endif
}

/* send_all / recv_all：处理短发送及 EINTR/EAGAIN */
ssize_t send_all(int fd, const void *buf, size_t len) {
    size_t left = len;
    const char *p = buf;
    while (left > 0) {
        ssize_t s = send(fd, p, left, 0);
        if (s < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* blocking socket normally shouldn't hit this, but retry a bit */
                struct timespec ts = {0, 1000000}; /* 1ms */
                nanosleep(&ts, NULL);
                continue;
            }
            return -1;
        }
        if (s == 0) return -1;
        p += s;
        left -= s;
    }
    return (ssize_t)len;
}

/* recv_all: 尝试读取 len 字节，若对端提前关闭或错误则返回 -1 */
ssize_t recv_all(int fd, void *buf, size_t len) {
    size_t left = len;
    char *p = buf;
    while (left > 0) {
        ssize_t r = recv(fd, p, left, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct timespec ts = {0, 1000000};
                nanosleep(&ts, NULL);
                continue;
            }
            return -1;
        }
        if (r == 0) {
            /* peer closed */
            return -1;
        }
        p += r;
        left -= r;
    }
    return (ssize_t)len;
}

/* 安全写进度：写到 tmp 再 rename */
static int write_progress_atomic(const char *fname, uint64_t val) {
    char prog[4096];
    char prog_tmp[4096];
    snprintf(prog, sizeof(prog), "%s.progress", fname);
    snprintf(prog_tmp, sizeof(prog_tmp), "%s.progress.tmp", fname);

    FILE *f = fopen(prog_tmp, "w");
    if (!f) return -1;
    if (fprintf(f, "%" PRIu64 "\n", val) < 0) {
        fclose(f);
        unlink(prog_tmp);
        return -1;
    }
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    /* 原子替换 */
    if (rename(prog_tmp, prog) != 0) {
        unlink(prog_tmp);
        return -1;
    }
    return 0;
}

/* 删除进度文件 */
static void remove_progress(const char *fname) {
    char prog[4096];
    snprintf(prog, sizeof(prog), "%s.progress", fname);
    unlink(prog);
}

/* 获取文件大小（从 stat），返回 -1 失败 */
static off_t get_file_size_stat(const char *fname) {
    struct stat st;
    if (stat(fname, &st) != 0) return -1;
    return st.st_size;
}

/* 客户端上传 — 注意：按你的要求 upload 不读取本地 progress 偏移，
   仅发送文件大小，等待服务器给出 agreed_offset，然后从 agreed_offset 发送剩余数据。 */
int client_upload(int sock, const char *filename) {
    int rc = -1;
    FILE *fp = NULL;
    uint32_t mode_len_net, name_len_net;
    const char *mode = "upload";

    /* 1) send mode */
    uint32_t mode_len = (uint32_t)strlen(mode);
    mode_len_net = htonl(mode_len);
    if (send_all(sock, &mode_len_net, sizeof(mode_len_net)) != sizeof(mode_len_net)) {
        perror("send mode_len");
        goto out;
    }
    if (send_all(sock, mode, mode_len) != (ssize_t)mode_len) {
        perror("send mode");
        goto out;
    }

    /* 2) send filename */
    uint32_t name_len = (uint32_t)strlen(filename);
    name_len_net = htonl(name_len);
    if (send_all(sock, &name_len_net, sizeof(name_len_net)) != sizeof(name_len_net)) {
        perror("send name_len");
        goto out;
    }
    if (send_all(sock, filename, name_len) != (ssize_t)name_len) {
        perror("send filename");
        goto out;
    }

    /* 3) send filesize */
    off_t sz = get_file_size_stat(filename);
    if (sz < 0) {
        perror("stat file");
        goto out;
    }
    uint64_t filesize = (uint64_t)sz;
    uint64_t net_filesize = htonll(filesize);
    if (send_all(sock, &net_filesize, sizeof(net_filesize)) != sizeof(net_filesize)) {
        perror("send filesize");
        goto out;
    }

    /* 4) receive agreed_offset from server */
    uint64_t net_agreed;
    if (recv_all(sock, &net_agreed, sizeof(net_agreed)) != sizeof(net_agreed)) {
        fprintf(stderr, "recv agreed_offset failed\n");
        goto out;
    }
    uint64_t agreed = ntohll(net_agreed);
    if (agreed > filesize) {
        fprintf(stderr, "server agreed_offset (%" PRIu64 ") > filesize (%" PRIu64 ")\n", agreed, filesize);
        goto out;
    }

    /* 5) open local file and seek to agreed, then send remaining data and update .progress */
    fp = fopen(filename, "rb");
    if (!fp) {
        perror("fopen file");
        goto out;
    }
    if (fseeko(fp, (off_t)agreed, SEEK_SET) != 0) {
        perror("fseeko");
        goto out;
    }

    uint64_t total_sent = agreed;
    char buf[CHUNK];
    size_t nread;
    while ((nread = fread(buf, 1, sizeof(buf), fp)) > 0) {
        if (send_all(sock, buf, nread) != (ssize_t)nread) {
            perror("send_all file data");
            goto out;
        }
        total_sent += (uint64_t)nread;

        /* 原子写进度 */
        if (write_progress_atomic(filename, total_sent) != 0) {
            fprintf(stderr, "warning: write progress failed\n");
        }
    }
    if (ferror(fp)) {
        perror("fread");
        goto out;
    }

    /* 上传完成，删除进度文件 */
    remove_progress(filename);
    printf("Upload finished: sent=%" PRIu64 "\n", total_sent);
    rc = 0;

out:
    if (fp) fclose(fp);
    close(sock);
    return rc;
}

/* 客户端下载 — 发送本地已有偏移，接收 filesize & server_offset，然后接收数据写入并更新进度 */
int client_download(int sock, const char *filename) {
    int rc = -1;
    FILE *fp = NULL;
    uint32_t mode_len_net, name_len_net;
    const char *mode = "download";

    /* 计算本地已有偏移（如果文件存在） */
    off_t local_offset = 0;
    fp = fopen(filename, "r+b");
    if (!fp) {
        /* 不存在则创建 */
        fp = fopen(filename, "wb+");
        if (!fp) {
            perror("fopen local file");
            goto out;
        }
    } else {
        if (fseeko(fp, 0, SEEK_END) != 0) {
            perror("fseeko");
            goto out;
        }
        local_offset = ftello(fp);
    }

    /* 1) send mode */
    uint32_t mode_len = (uint32_t)strlen(mode);
    mode_len_net = htonl(mode_len);
    if (send_all(sock, &mode_len_net, sizeof(mode_len_net)) != sizeof(mode_len_net)) {
        perror("send mode_len");
        goto out;
    }
    if (send_all(sock, mode, mode_len) != (ssize_t)mode_len) {
        perror("send mode");
        goto out;
    }

    /* 2) send filename */
    uint32_t name_len = (uint32_t)strlen(filename);
    name_len_net = htonl(name_len);
    if (send_all(sock, &name_len_net, sizeof(name_len_net)) != sizeof(name_len_net)) {
        perror("send name_len");
        goto out;
    }
    if (send_all(sock, filename, name_len) != (ssize_t)name_len) {
        perror("send filename");
        goto out;
    }

    /* 3) send local_offset */
    uint64_t net_local_offset = htonll((uint64_t)local_offset);
    if (send_all(sock, &net_local_offset, sizeof(net_local_offset)) != sizeof(net_local_offset)) {
        perror("send local_offset");
        goto out;
    }

    /* 4) recv filesize and server_offset */
    uint64_t net_filesize, net_server_offset;
    if (recv_all(sock, &net_filesize, sizeof(net_filesize)) != sizeof(net_filesize)) {
        fprintf(stderr, "recv filesize failed\n");
        goto out;
    }
    if (recv_all(sock, &net_server_offset, sizeof(net_server_offset)) != sizeof(net_server_offset)) {
        fprintf(stderr, "recv server_offset failed\n");
        goto out;
    }
    uint64_t filesize = ntohll(net_filesize);
    uint64_t server_offset = ntohll(net_server_offset);

    if (server_offset > filesize) {
        fprintf(stderr, "server_offset > filesize\n");
        goto out;
    }

    /* seek to server_offset for writing */
    if (fseeko(fp, (off_t)server_offset, SEEK_SET) != 0) {
        perror("fseeko to server_offset");
        goto out;
    }

    /* 5) receive file bytes until total_received == filesize or peer closes */
    uint64_t total_received = server_offset;
    char buf[CHUNK];
    while (total_received < filesize) {
        ssize_t want = (filesize - total_received) > CHUNK ? CHUNK : (ssize_t)(filesize - total_received);
        ssize_t r = recv_all(sock, buf, want);
        if (r != want) {
            fprintf(stderr, "recv failed or connection closed prematurely\n");
            goto out;
        }
        size_t w = fwrite(buf, 1, r, fp);
        if (w != (size_t)r) {
            perror("fwrite");
            goto out;
        }
        total_received += (uint64_t)r;
        fflush(fp);
        fsync(fileno(fp));

        /* 更新进度 */
        if (write_progress_atomic(filename, total_received) != 0) {
            fprintf(stderr, "warning: write progress failed\n");
        }
    }

    /* 下载完成，删除进度文件 */
    remove_progress(filename);
    printf("Download complete: %s (size=%" PRIu64 ")\n", filename, filesize);
    rc = 0;

out:
    if (fp) fclose(fp);
    close(sock);
    return rc;
}

int main(int argc, char *argv[]) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s upload|download <server_ip> <server_port> <filename>\n", argv[0]);
        return 1;
    }
    const char *mode = argv[1];
    const char *server_ip = argv[2];
    int server_port = atoi(argv[3]);
    const char *filename = argv[4];

    if (!(strcmp(mode, "upload") == 0 || strcmp(mode, "download") == 0)) {
        fprintf(stderr, "mode must be 'upload' or 'download'\n");
        return 1;
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    struct sockaddr_in serv;
    memset(&serv, 0, sizeof(serv));
    serv.sin_family = AF_INET;
    serv.sin_port = htons((uint16_t)server_port);
    if (inet_pton(AF_INET, server_ip, &serv.sin_addr) <= 0) {
        fprintf(stderr, "inet_pton failed\n");
        close(sock);
        return 1;
    }
    if (connect(sock, (struct sockaddr*)&serv, sizeof(serv)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    if (strcmp(mode, "upload") == 0) {
        return client_upload(sock, filename);
    } else {
        return client_download(sock, filename);
    }
}

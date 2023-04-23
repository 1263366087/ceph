#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <hiredis/hiredis.h>
#include <iostream>
#include <openssl/sha.h>
#include <rados/librados.hpp>
#include <sstream>
#include <string.h>
#include <string>
#include <vector>
using namespace std;
// 用redis存下已经上传的文件大小
void save_uploaded_size_to_redis(redisContext *redis_conn, const std::string &key, size_t uploaded_size)
{
        stringstream ss;
        ss << "SET " << key << " " << uploaded_size;
        std::string s = ss.str();
        // sprintf(const_cast<char*>(s.c_str),"SET %s %zd",key.c_str(),uploaded_size);
        redisReply *reply = (redisReply *)redisCommand(redis_conn, s.c_str());
        if (reply == nullptr)
        {
                std::cerr << "Couldn't save uploaded size to Redis!" << std::endl;
                exit(EXIT_FAILURE);
        }
        freeReplyObject(reply);
}

// 获取已经传输的文件大小
size_t load_uploaded_size_from_redis(redisContext *redis_conn, const std::string &key)
{
        stringstream ss;
        ss << "GET " << key;
        std::string s = ss.str();
        size_t uploaded_size = 0;
        redisReply *reply = (redisReply *)redisCommand(redis_conn, s.c_str());
        if (reply != nullptr && reply->type == REDIS_REPLY_STRING)
        {
                // 转换成整形
                uploaded_size = std::stoull(reply->str);
        }
        freeReplyObject(reply);
        return uploaded_size;
}

// 将本地文件上传到Ceph池的函数
void upload_local_file_to_object(librados::IoCtx &io_ctx, const std::string &local_file_path, const std::string &object_name,
                                 redisContext *redis_conn, const std::string &uploaded_size_key)
{
        // 先用流打开文件
        std::ifstream local_file(local_file_path, std::ios::binary);
        if (!local_file.is_open())
        {
                std::cerr << "Couldn't open the local file!" << std::endl;
                exit(EXIT_FAILURE);
        }
        // 读取本地文件内容
        local_file.seekg(0, std::ios::end);
        // 获取当前文件读取位置
        std::streamsize file_size = local_file.tellg();
        // 获取已经上传的文件大小
        size_t uploaded_size = load_uploaded_size_from_redis(redis_conn, uploaded_size_key+"up");
        local_file.seekg(uploaded_size);
        const size_t buffer_size = 4096;
        std::vector<char> buffer(buffer_size);
        size_t read_bytes;
        while (!local_file.eof())
        {
                // 将文件读到bufffer去
                local_file.read(buffer.data(), buffer_size);
                // 返回上一次具体读了多少个字节数;
                read_bytes = static_cast<size_t>(local_file.gcount());

                if (read_bytes > 0)
                {
                        librados::bufferlist bl;
                        bl.append(buffer.data(), read_bytes);
                        // 从uploaded_size开始写
                        int ret = io_ctx.write(object_name, bl, read_bytes, uploaded_size);
                        if (ret < 0)
                        {
                                std::cerr << "Couldn't write object! error " << ret << std::endl;
                                exit(EXIT_FAILURE);
                        }
                        else
                        {
                                printf("Uping:%.2f%%\r", uploaded_size * 100.0 / file_size);
                                fflush(stdout);
                                std::cout << "Wrote " << read_bytes << " bytes to the object." << std::endl;
                                sleep(1);
                        }

                        uploaded_size += read_bytes;
                        save_uploaded_size_to_redis(redis_conn, uploaded_size_key, uploaded_size);
                }
        }
}

// 上传本地文件到Ceph池，支持断点续传
std::string local_file_path_to_upload = "md5.h";  // 更改为要上传的本地文件的路径
std::string object_name_to_upload = "myobject";   // 更改为要在Ceph池中创建的对象名称
std::string uploaded_size_key = "uploaded_size1"; // 更改为要在Redis中存储已上传数据量的键名

/* void upload_local_file_to_object(librados::IoCtx &io_ctx, const std::string &file_path, const std::string &object_name)
{
        std::ifstream input_file(file_path, std::ios::binary);
        if (!input_file.is_open())
        {
                std::cerr << "Couldn't open local file for reading! error " << std::endl;
                exit(EXIT_FAILURE);
        }

        // 读取本地文件内容
        input_file.seekg(0, std::ios::end);
        // 获取当前文件读取位置
        std::streamsize file_size = input_file.tellg();
        input_file.seekg(0, std::ios::beg);

        std::vector<char> file_buffer(file_size);
        input_file.read(file_buffer.data(), file_size);
        input_file.close();

        // 将文件内容写入Ceph对象
        librados::bufferlist write_buf;
        write_buf.append(file_buffer.data(), file_size);
        int ret = io_ctx.write_full(object_name, write_buf);
        if (ret < 0)
        {
                std::cerr << "Couldn't write object! error " << ret << std::endl;
                exit(EXIT_FAILURE);
        }

        std::cout << "Uploaded local file '" << file_path << "' to object '" << object_name << "'." << std::endl;
} */

// 我们将下载的文件保存到本地文件系统上的 "downloaded_object.txt" 文件中
// 保存的文件名local_file_path
std::string local_file_path = "downloaded_object.txt";

// 读取对象到本地文件的函数
/* void download_object_to_local_file(librados::IoCtx &io_ctx, const std::string &object_name, const std::string &file_path)
{
        librados::bufferlist read_buf;
        uint64_t object_size;
        time_t object_mtime;
        int ret;

        // 获取对象大小
        // 下载对象到本地文件
        ret = io_ctx.stat(object_name, &object_size, &object_mtime);
        if (ret < 0)
        {
                std::cerr << "Couldn't stat object! error " << ret << std::endl;
                exit(EXIT_FAILURE);
        }

        // 读取对象内容
        ret = io_ctx.read(object_name, read_buf, object_size, 0);
        if (ret < 0)
        {
                std::cerr << "Couldn't read object! error " << ret << std::endl;
                exit(EXIT_FAILURE);
        }

        // 将对象内容写入本地文件
        std::ofstream output_file(file_path, std::ios::binary);
        if (!output_file.is_open())
        {
                std::cerr << "Couldn't open local file for writing! error " << std::endl;
                exit(EXIT_FAILURE);
        }
        output_file.write(read_buf.c_str(), read_buf.length());
        output_file.close();

        std::cout << "Downloaded object '" << object_name << "' to local file '" << file_path << "'." << std::endl;
} */
// 分块下载对象到本地文件的函数
void download_object_to_local_file(librados::IoCtx &io_ctx, const std::string &object_name, const std::string &file_path)
{
        librados::bufferlist read_buf;
        uint64_t object_size;
        time_t object_mtime;
        int ret;
        uint64_t block_size = 4096;
        // 获取对象大小
        ret = io_ctx.stat(object_name, &object_size, &object_mtime);
        if (ret < 0)
        {
                std::cerr << "Couldn't stat object! error " << ret << std::endl;
                exit(EXIT_FAILURE);
        }

        // 打开本地文件
        std::ofstream output_file(file_path, std::ios::binary);
        if (!output_file.is_open())
        {
                std::cerr << "Couldn't open local file for writing! error " << std::endl;
                exit(EXIT_FAILURE);
        }

        // 分块读取对象内容并写入本地文件
        uint64_t offset = 0;
        while (offset < object_size)
        {
                uint64_t read_size = std::min(block_size, object_size - offset);
                ret = io_ctx.read(object_name, read_buf, read_size, offset);
                if (ret < 0)
                {
                        std::cerr << "Couldn't read object! error " << ret << std::endl;
                        output_file.close();
                        exit(EXIT_FAILURE);
                }

                // 将对象内容写入本地文件
                output_file.write(read_buf.c_str(), read_buf.length());

                // 更新偏移量
                offset += read_size;
        }

        // 关闭本地文件
        output_file.close();

        std::cout << "Downloaded object '" << object_name << "' to local file '" << file_path << "'." << std::endl;
}
int main(int argc, const char **argv)
{

        redisContext *redis_conn = redisConnect("127.0.0.1", 6379);
        if (redis_conn == nullptr || redis_conn->err)
        {
                if (redis_conn)
                {
                        std::cerr << "Redis connection error: " << redis_conn->errstr << std::endl;
                        redisFree(redis_conn);
                }
                else
                {
                        std::cerr << "Couldn't allocate Redis context" << std::endl;
                }
                exit(EXIT_FAILURE);
        }
        /*  // 上传本地文件到Ceph池，支持断点续传
         std::string local_file_path_to_upload = "local_file.txt"; // 更改为要上传的本地文件的路径
         std::string object_name_to_upload = "uploaded_object";    // 更改为要在Ceph池中创建的对象名称
         std::string uploaded_size_key = "uploaded_size";          // 更改为要在Redis中存储已上传数据量的键名
  */
        // upload_local_file_to_object(io_ctx, local_file_path_to_upload, object_name_to_upload, redis_conn, uploaded_size_key);

        int ret = 0;

        /* Declare the cluster handle and required variables. */
        librados::Rados cluster;
        char cluster_name[] = "ceph";
        char user_name[] = "client.admin";
        uint64_t flags = 0;

        /* Initialize the cluster handle with the "ceph" cluster name and "client.admin" user */
        {
                ret = cluster.init2(user_name, cluster_name, flags);
                if (ret < 0)
                {
                        std::cerr << "Couldn't initialize the cluster handle! error " << ret << std::endl;
                        return EXIT_FAILURE;
                }
                else
                {
                        std::cout << "Created a cluster handle." << std::endl;
                }
        }

        /* Read a Ceph configuration file to configure the cluster handle. */
        {
                ret = cluster.conf_read_file("/etc/ceph/ceph.conf");
                if (ret < 0)
                {
                        std::cerr << "Couldn't read the Ceph configuration file! error " << ret << std::endl;
                        return EXIT_FAILURE;
                }
                else
                {
                        std::cout << "Read the Ceph configuration file." << std::endl;
                }
        }

        /* Read command line arguments */
        {
                ret = cluster.conf_parse_argv(argc, argv);
                if (ret < 0)
                {
                        std::cerr << "Couldn't parse command line options! error " << ret << std::endl;
                        return EXIT_FAILURE;
                }
                else
                {
                        std::cout << "Parsed command line options." << std::endl;
                }
        }

        /* Connect to the cluster */
        {
                ret = cluster.connect();
                if (ret < 0)
                {
                        std::cerr << "Couldn't connect to cluster! error " << ret << std::endl;
                        return EXIT_FAILURE;
                }
                else
                {
                        std::cout << "Connected to the cluster." << std::endl;
                }
        }

        librados::IoCtx io_ctx;
        const char *pool_name = "device_health_metrics";

        {
                ret = cluster.ioctx_create(pool_name, io_ctx);
                if (ret < 0)
                {
                        std::cerr << "Couldn't set up ioctx! error " << ret << std::endl;
                        exit(EXIT_FAILURE);
                }
                else
                {
                        std::cout << "Created an ioctx for the pool." << std::endl;
                }
        }

        /* Write an object synchronously. */
        {
                librados::bufferlist bl;
                bl.append("Hello World!");
                ret = io_ctx.write_full("hw", bl);
                if (ret < 0)
                {
                        std::cerr << "Couldn't write object! error " << ret << std::endl;
                        exit(EXIT_FAILURE);
                }
                else
                {
                        std::cout << "Wrote new object 'hw' " << std::endl;
                }
        }
        // 上传文件函数
        // upload_local_file_to_object(io_ctx, local_file_path_to_upload, object_name_to_upload);
        upload_local_file_to_object(io_ctx, local_file_path_to_upload, object_name_to_upload, redis_conn, uploaded_size_key);
        // upload_local_file_to_object(io_ctx, local_file_path_to_upload, object_name_to_upload);
        /*
         * Add an xattr to the object.
         */
        {
                librados::bufferlist lang_bl;
                lang_bl.append("en_US");
                ret = io_ctx.setxattr("hw", "lang", lang_bl);
                if (ret < 0)
                {
                        std::cerr << "failed to set xattr version entry! error "
                                  << ret << std::endl;
                        exit(EXIT_FAILURE);
                }
                else
                {
                        std::cout << "Set the xattr 'lang' on our object!" << std::endl;
                }
        }

        /*
         * Read the object back asynchronously.
         */
        {
                librados::bufferlist read_buf;
                int read_len = 4194304;

                // Create I/O Completion.
                librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();

                // Send read request.
                ret = io_ctx.aio_read("myobject", read_completion, &read_buf, read_len, 0);
                if (ret < 0)
                {
                        std::cerr << "Couldn't start read object! error " << ret << std::endl;
                        exit(EXIT_FAILURE);
                }

                // Wait for the request to complete, and check that it succeeded.
                read_completion->wait_for_complete();
                ret = read_completion->get_return_value();
                if (ret < 0)
                {
                        std::cerr << "Couldn't read object! error " << ret << std::endl;
                        exit(EXIT_FAILURE);
                }
                else
                {
                        std::cout << "Read object hw asynchronously with contents.\n"
                                  << read_buf.c_str() << std::endl;
                }
        }

        /*
         * Read the xattr.
         */
        {
                librados::bufferlist lang_res;
                ret = io_ctx.getxattr("hw", "lang", lang_res);
                if (ret < 0)
                {
                        std::cerr << "failed to get xattr version entry! error "
                                  << ret << std::endl;
                        exit(EXIT_FAILURE);
                }
                else
                {
                        std::cout << "Got the xattr 'lang' from object hw!"
                                  << lang_res.c_str() << std::endl;
                }
        }

        // 下载文件函数
        download_object_to_local_file(io_ctx, object_name_to_upload, local_file_path);

        /*
         * Remove the xattr.
         */
        {
                ret = io_ctx.rmxattr("hw", "lang");
                if (ret < 0)
                {
                        std::cerr << "Failed to remove xattr! error "
                                  << ret << std::endl;
                        exit(EXIT_FAILURE);
                }
                else
                {
                        std::cout << "Removed the xattr 'lang' from our object!" << std::endl;
                }
        }

        /*
         * Remove the object.
         */
        {

                ret = io_ctx.remove("hw");
                if (ret < 0)
                {
                        std::cerr << "Couldn't remove object! error " << ret << std::endl;
                        exit(EXIT_FAILURE);
                }
                else
                {
                        std::cout << "Removed object 'hw'." << std::endl;
                }
        }
        // 断开Redis连接
        redisFree(redis_conn);
        return 0;
}
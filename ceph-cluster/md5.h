#include<stdlib.h>
#include<stdio.h>
#include<unistd.h>
#include<fcntl.h>
#include<string.h>
#include<openssl/md5.h>
#include<stdbool.h>
#define MD5_LEN 16
#define BUFF_SIZE 1024*16

void md5_fun(char*filename,unsigned char*buffmd5);

//int Is_same(unsigned char* buff1,unsigned char*buff2);
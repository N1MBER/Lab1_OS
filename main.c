#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#define A 142
#define B 0x7FBC24
#define C malloc
#define D 44
#define E 57
#define F nocache
#define G 102
#define H random
#define I 147
#define J max
#define K cv

#define FILE_FLAG O_WRONLY | O_CREAT | O_TRUNC | O_DSYNC

typedef struct{
    size_t data_size;
    unsigned char* start_address;
    FILE * urandom;
} BufferArgs;

typedef struct{
    pthread_mutex_t * mutex;
    pthread_cond_t * cv;
    unsigned int count_files;
}ThreadArgs;

void* setBuffer(void* set_args);
void writeToFile(unsigned int count_files, unsigned char *buffer);
void* writeToMemory(unsigned char* buffer, unsigned int count_files, pthread_mutex_t * mutex, pthread_cond_t * cv);
long searchMax(unsigned char *block);
_Noreturn void* readAndExecute(void * args);

int main() {
    printf("До аллокации");
    getchar();
    int count_files = A / E + (A % E > 0 ? 1:0);
    unsigned char* buffer;
    buffer = C(A*1024*1024);

    printf("После аллокации");
    getchar();

    pthread_mutex_t mutex;
    pthread_cond_t cv;

    pthread_mutex_init(&mutex,NULL);
    pthread_cond_init(&cv,NULL);

    writeToMemory(buffer, count_files, &mutex, &cv);

    printf("После заполнения участка данными");
    getchar();

    free(buffer);
    printf("После деаллокации");
    getchar();

    buffer = C(A*1024*1024);
    pthread_t read_threads[I];

    for(int i =0;i<75;i++){
        ThreadArgs args = {&mutex, &cv, count_files};
        pthread_create(&read_threads[i], NULL, readAndExecute, &args);
    }
    while(1){
        writeToMemory(buffer, count_files, &mutex, &cv);
    }
    return 0;
}


void* setBuffer(void* set_args){
    BufferArgs *args = (BufferArgs*) set_args;

    for(size_t i =0;i<args->data_size;){
        i+=fread(args->start_address + i,1,args->data_size-i,args->urandom);
    }

    return NULL;
}

void writeToFile(unsigned int count_files, unsigned char *buffer){
    for(int i =0;i<count_files;i++){
        char file_name[14];
        snprintf(file_name,14,"file_%d.bin",i);
        int fd = open(file_name,FILE_FLAG,0666);
        size_t file_size = E * 1024 * 1024;
        
        for (size_t j = 0;j<file_size;){
            unsigned int block = rand() % (A*1024*1024/G);
            unsigned int block_size = file_size - j < G ? file_size - j : G;
            j+=write(fd, buffer+block*G, block_size);
        }
    }

}

void* writeToMemory(unsigned char* buffer, unsigned int count_files, pthread_mutex_t * mutex, pthread_cond_t * cv){
    pthread_t threads[D];
    unsigned char* start = buffer;
    FILE * urandom = fopen("/dev/urandom", "rb");
    size_t size_of_data = A*1024*1024/D;
    
    for(int i = 0;i<D-1;i++){
        BufferArgs args = {size_of_data, start, urandom};
        pthread_create(&threads[i], NULL, setBuffer, &args);
        start+=size_of_data;
    }
    
    BufferArgs args ={size_of_data + A * 1024 * 1024 % D, start, urandom};
    pthread_create(&threads[D - 1], NULL, setBuffer, &args);

    for(int i = 0; i < D; i++)
        pthread_join(threads[i], NULL);

    fclose(urandom);
    pthread_mutex_lock(mutex);
    writeToFile(count_files, buffer);
    pthread_cond_broadcast(cv);
    pthread_mutex_unlock(mutex);

}

long searchMax(unsigned char *block){
    int max = INT16_MIN;
    
    for (int i =0;i<G/sizeof(int);i+=sizeof(int)){
        int num = 0;
        
        for(int j = 0; j<sizeof(int);j++){
            num = (num << 8) + block[i+j];
        }
        
        if (num > max)
            max = num;
    }
    
    return max;
}

_Noreturn void* readAndExecute(void * args){
    ThreadArgs * arg = (ThreadArgs*) args;
    char file_name[14];
    snprintf(file_name,14,"file_%d.bin",arg->count_files-1);
    
    while(1){
        int max = INT16_MIN;
        pthread_mutex_lock(arg->mutex);
        pthread_cond_wait(arg->cv,arg->mutex);
        printf("%ld в состоянии ожидания\n", pthread_self());
        FILE *file = fopen(file_name,"rb");
        unsigned char block_size[G];
        
        for(unsigned int i = 0;i<2*E*1024*1024/G;i++){
            unsigned int block = rand()%(2*E*1024*1024/G);
            fseek(file,block*G,0);
            if(fread(&block_size,1,G,file) !=G)
                continue;
            else
                max = searchMax(block_size);

        }
        
        printf("Максимум %lld\n",max);
        fclose(file);
        pthread_mutex_unlock(arg->mutex);
    }
}

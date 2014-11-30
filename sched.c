/* Universidade Federal da Bahia - UFBA
 * Alunos: Evaldo Moreira, Lucas Borges, Thiago Pacheco
 * 
 */
 
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>


#include <unistd.h>
#include <semaphore.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <time.h>
#include <sys/mman.h>


#define QUANTUM 1	//unidade de tempo de tarefa, 1 segundo de execucao


typedef struct t{
	int dados[5];
	/*
	 	int id;					indice 0
		int instante_chegada;	...
		int tempo_execucao;		...
		int inicio_IO;			...
		int termino_IO;			indice 4
	 */
	struct t *next;
}Tarefa;


FILE *input, *output;
Tarefa *tarefas_input, *tarefas_output, *lista_tarefas, *tarefa;	//tarefa =  compartilhada
int *instante,  n_processos;


sem_t *mutex;
sem_t *mutex2; 


void FirstComeFirstServed();
void RoundRobin();
void ShortestJobFirst();
void lerEntradasArquivo();
int tamanho_alloc();
void getTarefas(int n);
Tarefa *criar_tarefas(Tarefa *t, int n);
Tarefa *alocarTarefa();
void fprinTest(Tarefa *t);

// execucao: ./sched RR input.txt output.txt
//simular execucao com comando sleep
//algoritmos: FCFS, RR, SJF  (first come first served, round robin, shortest job first)
//3 = ID, 0 = instante de chegada, 10 = tempo de execucao, comecou a fazer IO no instante 1 e terminou no instante 5

//compilar:
//gcc -pthread sched.c -o sched


int main(int argc, char *argv[]) {
	int tempo_total = 0, shmID_1, shmID_2, status;
	float tempo_medio = 0;
	long pid_filho1, pid_filho2;
	key_t key_1 = 567194, key_2 = 567195;

	
	input = fopen(argv[2], "r");
	n_processos = tamanho_alloc();			//numero de processos a serem simulados
	fclose(input);
		
	
	//compartilhar tarefa e instante entre os processos
	instante = (int*) malloc (sizeof(int));
	shmID_2 = shmget(key_2, sizeof(int), IPC_CREAT | 0666);
	instante = shmat(shmID_2, NULL, 0);
	*instante = 0;
	
	tarefas_input = criar_tarefas(tarefas_input, n_processos);		//armazena entrada do arquivo

	tarefa = (Tarefa*) malloc(sizeof(Tarefa));
	if ( (shmID_1 = shmget(key_1, sizeof(Tarefa), IPC_CREAT | 0666)) < 0 ){
		perror("shmget error");
		exit(1);
	}
	if( (tarefa = shmat(shmID_1, NULL, 0)) == (Tarefa*) -1){
		perror("shmat error");
		exit(1);
	}
	
	
	
	mutex  = mmap(NULL,sizeof(sem_t), PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
	mutex2 = mmap(NULL,sizeof(sem_t), PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
	
	if(!mutex || !mutex2)
		perror("mmap error");
		
		
	//iniciados em 0 e 1
	sem_init(mutex, 1, 0);
	sem_init(mutex2, 1, 1);
	
	
	pid_filho2 = fork();
	if(pid_filho2 != 0){
		pid_filho1 = fork();
		wait(&status);
	}
	
	
	//processo CPU
	 if(pid_filho1 == 0){
		fprintf(stderr, "[Processo CPU]...\n");
		 int i;
		 tarefas_output = (Tarefa*) calloc(n_processos, sizeof(Tarefa));
		 for(i=0; i < n_processos; i++){
			tarefas_output[i].dados[0] = i;
			tarefas_output[i].dados[1] = -1;
		}
		
		 while(1){
			 fprintf(stderr,"\n\nVALOR: ");
			 fprinTest(tarefa);
			 sem_wait(mutex);
			 if(tarefa->dados[2] == 0){
				fprintf(stderr, "Tarefa null no instante %d\n", *instante);
				break;
			}
			 fprintf(stderr, "[Processo CPU] Executando Tarefa {%d;%d;%d;%d,%d}...\n", tarefa->dados[0], tarefa->dados[1], tarefa->dados[2], tarefa->dados[3], tarefa->dados[4]);
			 //sleep(QUANTUM);
			 tarefa->dados[2] -= QUANTUM;		//tempo restante de execucao
			
			 tempo_total += QUANTUM;
			
			 if(tarefas_output[ tarefa->dados[0] ].dados[1] == -1)
				 tarefas_output[ tarefa->dados[0] ].dados[1] = (*instante);
			 else
			 	tarefas_output[ tarefa->dados[0] ].dados[2] += QUANTUM;
			
			 sem_post(mutex2);
			  
		 }
		 
		 fprintf(stderr,"\n>>>>>>>>>>>>>>>ENDWHILE\n");
		 
		 //salvar saida
		 output = fopen(argv[3], "w");
		 for(i=0; i < n_processos; i++)
			fprintf(stderr, "%d;%d;%d;\n", tarefas_output[i].dados[0], tarefas_output[i].dados[1], tarefas_output[i].dados[2]);
			//output 
		 fprintf(stderr, "Tempo Total: %d\nTempo medio: %f", tempo_total, tempo_medio);
		 fclose(output);
		 exit(0);
		 
	 }
	 
	 
	 //processo escalonador
	 if(pid_filho2 == 0){
		 fprintf(stderr, "\n[Processo Escalonador]...\n");
		 input = fopen(argv[2], "r");
		 lerEntradasArquivo();
	
		 //fprinTest(tarefas_input);
					 
		 if(!strcmp(argv[1], "FCFS"))
			 FirstComeFirstServed();
		 else if(!strcmp(argv[1], "RR"))
			 RoundRobin();
		 else if(!strcmp(argv[1], "SJF"))
			 ShortestJobFirst();

		 fclose(input);
		 exit(0);
	 }
	 
	 
	sem_destroy(mutex);
	munmap(mutex, sizeof(sem_t));
	sem_destroy(mutex2);
	munmap(mutex2, sizeof(sem_t));
	
	return EXIT_SUCCESS;
	
}




/* ########################################################## */
/* ########################################################## */
/* 							FUNCOES							 */
/* ########################################################## */




void FirstComeFirstServed(){

}

/* ########################################################## */

void RoundRobin(){
	int escalonando = 1;
	Tarefa *aux, *aux2;

	
	while(escalonando){
		sem_wait(mutex2);
		getTarefas(*instante);
		*tarefa = *lista_tarefas;

		fprintf(stderr, "Escalonando tarefa %d no inst %d\n", tarefa->dados[0], *instante);
		fprintf(stderr, "{Tarefa = %d;%d;%d,%d,%d;} | ",tarefa->dados[0],tarefa->dados[1],tarefa->dados[2],tarefa->dados[3],tarefa->dados[4]);
		fprintf(stderr, "Lista = ");
		fprinTest(lista_tarefas);
	
		sem_post(mutex);
		//esperar execucao
		sem_wait(mutex2);
		
		*lista_tarefas = *tarefa;
				
		if(tarefa->dados[2]==0){		//tempo de execucao = 0
			aux = lista_tarefas;
			lista_tarefas = lista_tarefas->next;
			free(aux);
			 
		}
		else
		{	//colocar tarefa executada no final da fila
			aux = lista_tarefas;
			while(aux->next!=NULL)
				aux = aux->next;
				
			aux->next = lista_tarefas;
			aux2 = lista_tarefas;
			lista_tarefas = lista_tarefas->next;
			aux2->next = NULL;
			
			//fprinTest(lista_tarefas);
			
			}		
	
			if(lista_tarefas==NULL){
				escalonando = 0;
				//tarefa->dados[0] = -1;
				fprintf(stderr,"\n\n_________NULL ____");
				fprinTest(tarefa);
				
			}

			(*instante)++;
			sem_post(mutex2);

		}

}

/* ########################################################## */

void ShortestJobFirst(){

}

/* ########################################################## */

void lerEntradasArquivo(){
	char c, temp[100];
	int i = 0, j = 0;
	Tarefa *aux = tarefas_input;

	rewind(input);


	while( (c = fgetc(input) ) != EOF){
		if(c == '\n'){
			aux = aux->next;
			i=0;
			j=0;
		}
		else
		{	if(isdigit(c)){
				temp[j++] = c;
				temp[j] = '\0';

			}
			else
			{
				aux->dados[i++] = atoi(temp);
				memset(temp, 0, 100);
				j=0;
			}// ( c == ; )
		}// (c != \n)
	}//end while
}

/* ########################################################## */

void getTarefas(int n){
	Tarefa *input_aux = tarefas_input, *aux2, *ant=NULL;


	
	if(lista_tarefas==NULL){
		lista_tarefas = criar_tarefas(lista_tarefas,1);
		aux2 = lista_tarefas;
	}
	else if(n <= n_processos){
		aux2 = lista_tarefas;
		while(aux2!=NULL){
			ant = aux2;
			aux2 = aux2->next;
		}
		
	}
	else return;
	
	
	while(input_aux!=NULL){
		fprintf(stderr, "{Input %d;%d;%d;%d,%d}\n", input_aux->dados[0],input_aux->dados[1],input_aux->dados[2],input_aux->dados[3],input_aux->dados[4]);
		if(input_aux->dados[1] == n){				//instante de chegada
			fprintf(stderr, "Instante %d chega processo %d\n", n, input_aux->dados[0]);
			if(aux2==NULL)
				aux2 = criar_tarefas(aux2, 1);
			*aux2 = *input_aux;
			aux2->next = NULL;
			if(ant!=NULL)
				ant->next = aux2;
			ant = aux2;
			aux2 = aux2->next;
		}
		
		input_aux = input_aux->next;

	}

}

/* ########################################################## */

int tamanho_alloc(){
	char c;
	int i = 1;

	while ((c = fgetc(input)) != EOF)
		if( c == '\n'){
			i++;
		}

	return i;
}

/* ########################################################## */

Tarefa *criar_tarefas(Tarefa *t, int n){
	int i;
	Tarefa *aux, *ant;

	aux = alocarTarefa();

	t = aux;
	ant = aux;
	aux = aux->next;


	for(i=1; i < n; i++){
		aux = alocarTarefa();
		ant->next = aux;
		ant = aux;
		aux = aux->next;
	}

	return t;
}

/* ########################################################## */

Tarefa *alocarTarefa(){
	Tarefa *temp;
	int i;

	temp = (Tarefa*) malloc (sizeof(Tarefa));
	temp->next = NULL;

	for(i=0; i < 5; i ++)
		temp->dados[i] = 0;

	return temp;
}

/* ########################################################## */

void fprinTest(Tarefa *t){
	Tarefa *aux = t;
	
	
	while(aux!=NULL){
		fprintf(stderr, "{%d;%d;%d;%d,%d}  ", aux->dados[0],aux->dados[1],aux->dados[2], aux->dados[3], aux->dados[4]);
		aux = aux->next;
	}
	fprintf(stderr, "\n");
}

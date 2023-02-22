#include <iostream>
#include <fstream>
#include <string>
#include <pthread.h>
#include <math.h>
#include <unordered_set>

using namespace std;
void *status[5];

// We define a struct that contains all the lists with numbers for a thread,
// and the length of every such list. Every mapper thread will return one
// such structure to be the result of their calculation.
typedef struct lists_with_numbers {
    int **lists;
    int *number_of_elements_in_list;
    int *crt_capacity_of_list;
}lists_with_numbers_t;

typedef struct map_arguments{
    lists_with_numbers_t *lists;
    int no_reducers;
    int *remaining_files;
    ifstream *input_file;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
}map_arguments_t;

typedef struct reduce_arguments{
    int id, no_mappers;
    lists_with_numbers_t **map_lists;
    pthread_barrier_t *barrier;
}reduce_arguments_t;

// The function initializes the partial lists of a mapper thread.
lists_with_numbers_t initialize_list (int no_reducers) {
    lists_with_numbers_t lists;

    lists.number_of_elements_in_list = (int *) malloc (no_reducers * sizeof (int));
    lists.crt_capacity_of_list = (int *) malloc (no_reducers * sizeof (int));
    lists.lists = (int **) malloc (no_reducers * sizeof (int *));

    if (lists.number_of_elements_in_list == NULL || lists.crt_capacity_of_list == NULL ||
        lists.lists == NULL) {
            cout << "There was a problem at allocation" << endl;
            exit (3);
        }

    for (int i = 0; i < no_reducers; i++) {
        lists.lists[i] = NULL;
        lists.number_of_elements_in_list[i] = 0;
        lists.crt_capacity_of_list[i] = 0;
    }

    return lists;
}

// The function verifies if there is an x where x^exponent == number, where x
// is an integer. If x exists, the function returns true, otherwise false.
// 1 raised to every power is 1, so it returns instantly true.
bool verifyIfPower(int number, int exponent) {
    if (number == 1) return true;

    long low = 1;

    // There is a demonstration on the Internet that the searched root is no
    // bigger than this.
    long high = 1 << ((int)log2(number) / exponent + 1);
    long mid = (high + low) / 2;

    while (low < high - 1) {
        long power = pow(mid, exponent);

        if (power < number) low = mid;
        else if (power > number) high = mid;
        else return true;

        mid = (low + high) / 2;
    }

    return false;
}

// The function receives the lists with numbers arranged based on perfect powers
// and a file name, and adds to the respective category the new numbers.
void add_numbers_to_lists (lists_with_numbers_t lists, string file_name,
                           int no_reducers) {

    string line;
    ifstream input_file (file_name);
    int no_of_lists;

    // We get the number of numbers of the current file
    input_file >> line;
    no_of_lists = stoi (line);


    for (int i = 0; i < no_of_lists; i++) {
        input_file >> line;
        int crt_number = stoi (line);

        for (int j = 0; j < no_reducers; j++) {
            if (verifyIfPower(crt_number, j + 2)) {
                lists.number_of_elements_in_list[j]++;
                int number = lists.number_of_elements_in_list[j];
                int capacity = lists.crt_capacity_of_list[j];

                if (number > capacity) {

                    // If the list doesn't have enough capacity, then
                    // we double it, so we do less memory work.
                    if (capacity == 0){
                        lists.crt_capacity_of_list[j] = 1;
                        capacity = 1;
                        lists.lists[j] = (int *) malloc (sizeof (int));
                    }
                    else {
                        int *aux;

                        lists.crt_capacity_of_list[j] *= 2;
                        capacity *= 2;
                        aux = (int *) realloc (lists.lists[j],capacity * sizeof (int));

                        if (aux == NULL) {
                            cout << "There was a problem at allocation" << endl;
                            exit (3);
                        }

                        lists.lists[j] = aux;
                    }
                }

                lists.lists[j][number - 1] = crt_number;
            }
        }
    }
    
    input_file.close();
}

// The mappers will have a list with numbers, based on the number of reducers,
// and to be sure that we read every line from the file once, and only once,
// we will also have a number of remaining files that need to be analyzed,
// in order to know if should stop reading or not. The number of remaining files
// will be the same for all the mapper threads.
map_arguments_t* create_arguments_for_mappers (int *no_files, int no_reducers,
                                             pthread_mutex_t *mutex, 
                                             pthread_barrier_t *barrier, 
                                             ifstream *file,
                                             lists_with_numbers_t *lists) {

    map_arguments_t *args = (map_arguments_t *) malloc (sizeof (map_arguments_t));

    if (args == NULL) {
        cout << "There was a problem at allocation" << endl;
    }

    args->lists = lists;
    args->no_reducers = no_reducers;
    args->remaining_files = no_files;
    args->mutex = mutex;
    args->input_file = file;
    args->barrier = barrier;

    return args;
}

// The function creates all the arguments for the reducer threads that will go
// in the structure passed as parameter. For every reducer we need an id to
// know which list we care about, the lists resulted from the mappers and their
// number, and also the barrier, in order to synchronize.
reduce_arguments_t* create_argumenents_for_reducers (int id, int no_mappers,
                                                     lists_with_numbers_t **map_lists,
                                                     pthread_barrier_t *barrier) {

    reduce_arguments_t *args = (reduce_arguments_t *) malloc (sizeof (reduce_arguments_t));
    if (args == NULL) {
        cout << "There was a problem at allocation" << endl;
        exit (3);
    }

    args->id = id;
    args->no_mappers = no_mappers;
    args->map_lists = map_lists;
    args->barrier = barrier;

    return args;
}

// The function creates all the lists that need to be filled for every mapper.
lists_with_numbers_t **create_lists_for_mappers (int no_mappers, int no_reducers) {
    lists_with_numbers_t **all_lists;

    all_lists = (lists_with_numbers_t **) malloc (no_mappers * sizeof (lists_with_numbers_t *));
    if (all_lists == NULL) {
        cout << "There was a problem at allocation" << endl;
        exit (3);
    }

    for (int i = 0; i < no_mappers; i++) {
        all_lists[i] = (lists_with_numbers_t *) malloc (sizeof (lists_with_numbers_t));

        if (all_lists[i] == NULL) {
            cout << "There was a problem at allocation" << endl;
            exit (3);
        }

        *all_lists[i] = initialize_list (no_reducers);
    }

    return all_lists;
}

// The function receives a map_arguments_t that contains the input file, and
// some other useful informations. It also contains the number of remaining
// files that need to be read. When a mapper finishes a file, it verifies
// if this number is 0, and if it is not, it takes the next file and decrements
// the number. The whole process of verifying if there are more files is locked
// by a mutex in order to avoid two threads to process the same file.
void *mappers_function(void *args) {
    map_arguments_t* arguments = (map_arguments_t *) args;
    bool try_to_read = true;
    string line;

    while (try_to_read) {
        try_to_read = false;
        pthread_mutex_lock (arguments->mutex);
        if (*(arguments->remaining_files) > 0){
            try_to_read = true;
            *(arguments->remaining_files) -= 1;

            *(arguments->input_file) >> line;
        }
        
        pthread_mutex_unlock (arguments->mutex);

        if (try_to_read) {
            add_numbers_to_lists (*arguments->lists, line, arguments->no_reducers);
        }
    }

    pthread_barrier_wait (arguments->barrier);

    pthread_exit (NULL);
}

// For reducers we use a barrier at the begining to wait for the mappers to
// finish. When they are ready, we open the output file. We don't need to be
// careful because every thread works with another file. We use an unordered_set
// that is implemented as a hash map. We go through every mapper's lists, and
// look only at the list that interests us based on the thread id. For every
// number, we verify if we have it in the set, if not we add it and increment
// the counter, otherwise we just go to the next one.
void *reducers_function(void * args) {
    reduce_arguments_t *arguments = (reduce_arguments_t *) args;

    pthread_barrier_wait (arguments->barrier);
    lists_with_numbers_t **lists = arguments->map_lists;
    unordered_set<int> set;
    int counter = 0;
    int id = arguments->id;
    char file_name[9] = "outE.txt";

    // We modify the file's name, so it matches the desired name.
    file_name[3] = (id + 2) + '0';

    ofstream output_file (file_name);

    for (int i = 0; i < arguments->no_mappers; i++) {
        for (int j = 0; j < lists[i]->number_of_elements_in_list[id]; j++) {
            if (set.find(lists[i]->lists[id][j]) == set.end()) {
                set.insert (lists[i]->lists[id][j]);
                counter++;
            }
        }
    }

    output_file << counter;
    output_file.close();

    pthread_exit(NULL);
}

// The funcion creates the mappers threads and reducers threads, all at once.
void create_threads(pthread_t *mappers, int no_mappers,
                    pthread_t *reducers, int no_reducers,
                    int *no_files, ifstream *file, pthread_mutex_t *mutex,
                    pthread_barrier_t *barrier) {

    lists_with_numbers_t **mappers_lists = create_lists_for_mappers (no_mappers, no_reducers);

    for (int i = 0; i < no_mappers + no_reducers; i++) {
        if (i < no_mappers) {
            map_arguments_t* args = create_arguments_for_mappers (no_files, no_reducers, mutex, barrier, file, mappers_lists[i]);
            pthread_create (&mappers[i], NULL, mappers_function, args);
        } 
        else {
            reduce_arguments_t *args = create_argumenents_for_reducers (i - no_mappers, no_mappers, mappers_lists, barrier);
            pthread_create (&reducers[i - no_mappers], NULL, reducers_function, args);
        }
    }
}

// The function makes the main thread to wait for the children threads to finish
// their execution.
void join_threads(pthread_t *mappers, int no_mappers, pthread_t *reducers,
                  int no_reducers) {

    for (int i = 0; i < no_mappers + no_reducers; i++) {
        if (i < no_mappers) {
            pthread_join (mappers[i], &status[i]);
        }
        else {
            pthread_join (reducers[i - no_mappers], NULL);
        }
    }
}

int main(int argc, char **argv) {
    int no_mappers, no_reducers, no_files;
    pthread_t *mappers, *reducers;
    pthread_mutex_t mutex;
    pthread_barrier_t barrier;
    string line;

    // Getting the arguments.
    if (argc < 4) {
        cout << "Insufficient number of arguments: ./tema1 <no_of_mappers>"
        " <no_of_reducers> <input_file>" << endl;
        return 1;
    }

    no_mappers = atoi (argv[1]);
    no_reducers = atoi (argv[2]);
    ifstream input_file(argv[3]); 

    // Initializing the synchronazation tools.
    pthread_mutex_init (&mutex, NULL);  
    pthread_barrier_init (&barrier, NULL, no_reducers + no_mappers);

    if (input_file.fail()) {
        cout << "There was a problem with opening/finding the input file" << endl;

        return 2;
    }

    // Getting the number of files.
    input_file >> line;
    no_files = stoi (line);

    // Allocating and creating the threads.
    mappers = (pthread_t *) malloc (no_mappers * sizeof(pthread_t));
    reducers = (pthread_t *) malloc (no_reducers * sizeof(pthread_t));

    // Verify if the malloc was successful
    if (mappers == NULL || reducers == NULL) {
        cout << "Problems at allocating memory for the threads" << endl;
        exit (3);
    }

    create_threads (mappers, no_mappers, reducers,
                    no_reducers, &no_files, &input_file, &mutex, &barrier);

    // Wait for the threads to finish.
    join_threads (mappers, no_mappers, reducers, no_reducers);

    // Deallocating the memory.
    free (mappers);
    free (reducers);

    input_file.close();

    return 0;
}
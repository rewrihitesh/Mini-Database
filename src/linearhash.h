#include<math.h>
/**
 * @brief 
 * Linear Hashing Data structure to implement indexing on a column in a table.
 * Dense clustering is performed.
 * Sorting is done by storing distinct values of keys inserted.
 * Initializer needs initial buckets and a bucket size.
 */

class Bucket{
public:
    unordered_map<int,set<int>> KeyValue;

    Bucket();
    
    void insert(int key, int value);
    void insert(int key, set<int> values);
    int remove(int key, int value);
    void print();
};

class Linearhash{
    int r=0;
    float threshold = 0.75;
    float min_th    = 0.25;
    int initial_buckets = 2;
    int Bucket_size = 4;

    vector<Bucket*> bucket;
    set<int> keys;

    public:
    Linearhash(int bsize,int initial_buckets);
    
    int hash(int key);
    set<int> get(int x);
    void insert(int key, int value);
    void update(int key, int value, int value2);
    void remove(int key, int value);
    void remove(int key);
    void print();
    vector<int> retrieveKeys(string order);
};
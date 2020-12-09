#include"global.h"

Bucket::Bucket(){
}

void Bucket::insert(int key, int value){
    this->KeyValue[key].insert(value); 
}
void Bucket::insert(int key, set<int> values){
    this->KeyValue[key]=values;
}

int Bucket::remove(int key, int value){
    int ret = 0;
    if(value<0){
        ret = KeyValue.size();
        this->KeyValue.erase(key);
    }
    else{
        ret = 1;
        this->KeyValue[key].erase(value);
        if(this->KeyValue[key].size()==0)
            this->KeyValue.erase(key);
    }
    return ret;
}

void Bucket::print(){
    for(auto i:this->KeyValue){
        cout<<i.first<<":";
        for(auto j:i.second){
            cout<<j<<",";
        }
        cout<<"|";
    }
}


Linearhash::Linearhash(int bsize,int initial_buckets){ 
    this->Bucket_size = bsize;
    this->initial_buckets = initial_buckets;
    for(int i=0;i<initial_buckets;i++)
        bucket.push_back(new Bucket());
}
/* Can change it further but as database is integer only key is integer */
int Linearhash::hash(int key){
    int i = ceil(log2((int)bucket.size()));
    /* no hash used */
    int k = key;      
    int m = k % (int)pow(2, i);
    if(m>=bucket.size())
        m = k % (int)pow(2, i-1);   
    return m;
}

set<int> Linearhash::get(int key){
    if(this->bucket[hash(key)]->KeyValue.find(key) != this->bucket[hash(key)]->KeyValue.end())
        return this->bucket[hash(key)]->KeyValue[key];
}
/** @brief function to insert a key(a value in indexcolumn) and record ptr in
 * Linear hash, Occupancy is checked and buckets are inserted dynamically.
 * 
 * @param key
 * @param value
 */
void Linearhash::insert(int key, int value){
    this->keys.insert(key);
    this->bucket[hash(key)]->insert(key,value);
    r++;
    /* check occupancy and increase buckets */
    if((float)r/(bucket.size()*Bucket_size) > threshold){
        /* Allocate new bucket */
        int n_ = bucket.size();
        int i = ceil(log2(n_));
        int ndash = n_ % (int)pow(2,i-1);
        bucket.push_back(new Bucket());

        /* Re-hash ndash bucket keys to n_, if they belong to new bucket n_ */
        int j = ceil(log2(n_+1)); 
        vector<int> temp_del;
        for(auto i:bucket[ndash]->KeyValue){
            int k = hash(i.first);
            int m = k % (int)pow(2,j);

            if(m == n_){
                bucket[n_]->insert(i.first, i.second);
                temp_del.push_back(i.first);
            }
        }
        for(int i=0;i<temp_del.size();i++) bucket[ndash]->remove(temp_del[i],-1);
    }
}

/** @brief function to remove a key(a value in indexcolumn) and record ptr in
 * Linear hash, Occupancy is checked and buckets are removed dynamically.
 * Overload function to support single record deletion.
 * Give value == -1 if all values with key = Key should be deleted.
 * 
 * @param key
 * @param value
 */
void Linearhash::remove(int key, int value){
    int d = this->bucket[hash(key)]->remove(key,value);
    r-=d;

    /* check occupancy and increase buckets */
    while(bucket.size()>this->initial_buckets && (float)r/(bucket.size()*Bucket_size) < min_th){
        /* Allocate new bucket */
        int n_ = bucket.size()-1;

        /* Re-hash n_ bucket keys to its suffix-1(.) (or) ndash bucket*/
        int j = ceil(log2(n_));
        int ndash = n_ % (int)pow(2,j-1);

        for(auto i:bucket[n_]->KeyValue)
            bucket[ndash]->insert(i.first,i.second);
        Bucket *head = bucket[n_];
        bucket.pop_back();
        delete head;
    }
}

void Linearhash::remove(int key){
    this->remove(key,-1);
}

/** @brief Function to retrieve the distinct values of the keys inserted in the hash.
 * 
 * @param order - ASC | DESC
 */
vector<int> Linearhash::retrieveKeys(string order){
    vector<int> ret;
    if(order == "ASC")
       for(auto i=keys.begin();i!=keys.end();i++)
            ret.push_back(*i);
    else
        for(auto i=keys.rbegin();i!=keys.rend();i++)
            ret.push_back(*i);
    return ret;
}

/**
 * @brief Function to update value for a key to value2.
 */
void Linearhash::update(int key, int value, int value2){
    this->bucket[hash(key)]->KeyValue[key].erase(value);
    this->bucket[hash(key)]->KeyValue[key].insert(value2);
}
void Linearhash::print(){
    int free=0;
    for(int i=0;i<bucket.size();i++){
        if(bucket[i]->KeyValue.size()!=0){
            bucket[i]->print();
            cout<<endl;
        }
        else free++;
    }
    cout<<"total buckets:"<<this->bucket.size()<<" free:"<<free<<endl;
}


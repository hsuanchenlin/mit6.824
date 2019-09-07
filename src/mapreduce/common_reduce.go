package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.

	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//


	//for _, k := range keys {
	//	fmt.Println("Key:", k, "Value:", m[k])
	//}

	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.

	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// Your code here (Part I).
	//
	tmpMap := make(map[string][]string)
	var kv KeyValue
	for i:=0;i<nMap;i++{
		fileName := reduceName(jobName, i, reduceTask)
		file, err := os.Open(fileName)
		if err != nil {
			log.Println(err.Error())
		}
		enc := json.NewDecoder(file)

		err = enc.Decode(&kv)

		for err == nil{
			_, ok := tmpMap[kv.Key]
			if !ok {
				tmpMap[kv.Key] = []string{kv.Value}
			} else {
				tmpMap[kv.Key] = append(tmpMap[kv.Key], kv.Value)
			}
			err = enc.Decode(&kv)
		}
		file.Close()
	}

	var keys []string
	for k, _ := range tmpMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	file1, _ := os.Create(outFile)
	defer file1.Close()
	enc1 := json.NewEncoder(file1)
	//for key := tmpList {
	//	enc1.Encode(KeyValue{key, reduceF(...)})
	//}
	for _, k := range keys {
		enc1.Encode(KeyValue{k, reduceF(k, tmpMap[k])})
	}
}

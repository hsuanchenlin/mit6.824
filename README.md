MIT 6.824 Course 
---

## Reference

[course website](https://pdos.csail.mit.edu/6.824/)

[version](https://github.com/aQuaYi/MIT-6.824-Distributed-Systems)


# Lab 1
## Part 1
不算難寫，只是要搞懂各個參數在做什麼
## Part 2
有讀懂作業說明就可以了，要注意mapF的結果KeyValue的Value是string
這邊就會是"1"，reduceＦ要轉換才能做加總 
## Part 3
- 如果不考慮Part 4, 蠻簡單的
這邊學會non buffered channel即可
取出task -> 分配worker -> 完成call wg.Add
## Part 4
- 覺得最難的一塊
- 本來還想試著用Mutex，但發現更麻煩，更不會寫...
- 必須用buffered和non buffered channel來完成
- 要兩層goroutine,第一層不斷取出worker task 並且因是non buffered channel，沒task時會被阻塞
-在內層裡，取到可用的worker和task(這邊要保證worker閒置 task未完成才能進來) 如果完成就使用wg.Add．最外圈main loop 使用wg.Wait
這樣一來 最內層完成所有task時 會讓mainloop結束 離開
## Part 5
聽過TFIDF，應該很好寫





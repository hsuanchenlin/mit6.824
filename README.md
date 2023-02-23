MIT 6.824 Course 
---

## Reference

[course website](https://pdos.csail.mit.edu/6.824/)

[Others's solution 1](https://github.com/aQuaYi/MIT-6.824-Distributed-Systems)

[Others's solution 2](https://github.com/wqlin/mit-6.824-2018)

[Youtube video playlist](https://www.youtube.com/playlist?list=PLkcQbKbegkMqiWf7nF8apfMRL4P4sw8UL)

[students-guide-to-raft](https://thesquareplanet.com/blog/students-guide-to-raft/)

[raft illustration](http://thesecretlivesofdata.com/raft/)

# Lab 1
## Part 1
- 不算難寫，只是要搞懂各個參數在做什麼
- Not difficult to write, but one needs to understand what each parameter is doing.

## Part 2
- 有讀懂作業說明就可以了，要注意mapF的結果KeyValue的Value是string
這邊就會是"1"，reduceF要轉換才能做加總 
- If you read and understand the instructions, it's easy. You need to be aware that the result of mapF for the KeyValue Value should be a string, which will be "1" in this case. The reduceF needs to be converted to perform summation.
## Part 3
- 如果不考慮Part 4, 蠻簡單的
這邊學會non buffered channel即可
取出task -> 分配worker -> 完成call wg.Add
- It's pretty simple if you don't consider Part 4. You just need to learn non-buffered channel. Take out the task -> assign worker -> complete call wg.Add.
## Part 4
- 覺得最難的一塊
- 本來還想試著用Mutex，但發現更麻煩，更不會寫...
- 必須用buffered和non buffered channel來完成
- 要兩層goroutine,第一層不斷取出worker task 並且因是non buffered channel，沒task時會被阻塞
-在內層裡，取到可用的worker和task(這邊要保證worker閒置 task未完成才能進來) 如果完成就使用wg.Add．最外圈main loop 使用wg.Wait
- 這樣一來 最內層完成所有task時 會讓mainloop結束 離開
I think this is the most challenging part. I originally wanted to try using Mutex, but I found it more complicated and difficult to write. You need to use buffered and non-buffered channels to accomplish this. Two layers of goroutines are required. The first layer continuously retrieves worker tasks, and because it is a non-buffered channel, it will be blocked if there is no task available. In the inner layer, get an available worker and task (make sure the worker is idle and the task is not completed) and use wg.Add if it is completed. The main loop in the outermost layer uses wg.Wait. This way, when all tasks are completed in the innermost layer, the main loop will end and exit.
## Part 5
- 聽過TFIDF，應該很好寫
- If you've heard of TFIDF, it should be easy to write.

# Lab 2
## A
很難寫
一開始寫了一版本 只能過第一個測試
後來參考別人的解答 修改程式架構 再修一些bug就過了
It's difficult to write. I wrote one version at first, but it only passed the first test. Later, I referred to other people's solutions, modified the program structure, and fixed some bugs, and then it passed.
## B



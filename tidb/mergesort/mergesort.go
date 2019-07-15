package main

import (
	"runtime"
	"sort"
	"sync"
)

func MergeSort(src []int64) {
	// sort.Slice(src, func(i, j int) bool {
	// 	return src[i] < src[j]
	// })
	MultiHeapSort(src)
}

var subSelectCnt int

// 方案1: 并发只进行最小堆初始化 -- 之后每次选择之后在调整 -- 并发优化效果不明显
// 方案2: 并发完成子序列排序，记录每次选择元素的子序列游标 参考 https://github.com/cch123/talent-plan/blob/master/tidb/mergesort/mergesort.go
// 方案3: 增加并发goroutine数，执行时间反而比NumCPU长一点点
func MultiHeapSort(arr []int64) {
	conc := runtime.NumCPU()
	var wg sync.WaitGroup
	subSize := len(arr) / conc
	subHeaps := make([]*subHeap, 0)
	for i := 0; i < conc; i++ {
		subHeap := subHeap{}
		start := i * subSize
		end := start + subSize
		if i == conc-1 {
			end = len(arr)
		}
		subHeap.len = end - start
		if subHeap.len > 0 {
			wg.Add(1)
			subSelectCnt++
			subHeap.data = make([]int64, subHeap.len)
			copy(subHeap.data, arr[start:end])
			subHeaps = append(subHeaps, &subHeap)
			go BuildOne(&subHeap, &wg)
		}
	}
	wg.Wait()
	BuildHeapSort(subHeaps, subSelectCnt)
	var i int = 0
	for {
		selected, existed := SelectOne(subHeaps)
		if existed {
			arr[i] = selected
			i++
			SubAdjustDown(subHeaps, 0, subSelectCnt)
		} else {
			break
		}
	}
}

type subHeap struct {
	data []int64
	len  int
	s    int
}

func (s *subHeap) adjust(parent int) {
	for {
		c := parent*2 + 1
		if c+1 < s.len && s.data[c] > s.data[c+1] {
			c++
		}
		// 没有子节点 || 满足最小堆
		if c >= s.len || s.data[parent] < s.data[c] {
			break
		}
		s.data[parent], s.data[c] = s.data[c], s.data[parent]
		parent = c
	}
}

func BuildOne(s *subHeap, wg *sync.WaitGroup) {
	// 初始化
	// for n := s.len/2 - 1; n >= 0; n-- {
	// 	s.adjust(n)
	// }
	sort.Slice(s.data, func(x, y int) bool { return s.data[x] < s.data[y] })
	wg.Done()
}

func SelectOne(arrs []*subHeap) (selected int64, existed bool) {
	if arrs[0].len == 0 {
		return
	}
	selected = arrs[0].data[arrs[0].s]
	existed = true
	// arrs[0].data[0], arrs[0].data[arrs[0].len-1] = arrs[0].data[arrs[0].len-1], arrs[0].data[0]
	arrs[0].len--
	arrs[0].s++
	if arrs[0].len == 0 {
		// mark remove
		arrs[0], arrs[subSelectCnt-1] = arrs[subSelectCnt-1], arrs[0]
		subSelectCnt--
	}
	return
}

// ------子序列按照第一个元素堆排序

func BuildHeapSort(arr []*subHeap, total int) {
	// 初始化
	for n := total/2 - 1; n >= 0; n-- {
		SubAdjustDown(arr, n, total)
	}
	// 调整/选取
	// for n := total - 1; n > 0; n-- {
	// 	arr[0], arr[n] = arr[n], arr[0]
	// 	SubAdjustDown(arr, 0, n)
	// }
}

func SubAdjustDown(arr []*subHeap, parent, total int) {
	for {
		c := parent*2 + 1
		if c+1 < total &&
			arr[c].len > 0 &&
			arr[c+1].len > 0 &&
			arr[c].data[arr[c].s] > arr[c+1].data[arr[c+1].s] {
			c++
		}
		// 没有子节点 || 满足最小堆 || len = 0为已删除的节点
		if c >= total || arr[c].len == 0 || arr[parent].data[arr[parent].s] < arr[c].data[arr[c].s] {
			break
		}
		arr[parent], arr[c] = arr[c], arr[parent]
		parent = c
	}
}

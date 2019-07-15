package main

import (
	"runtime"
	// "sort"
	"sync"
)

// func merge(src []int64, m int) {

// }

func MergeSort(src []int64) {
	// sort.Slice(src, func(i, j int) bool {
	// 	return src[i] < src[j]
	// })
	MultiHeapSort(src)
}

func MultiHeapSort(arr []int64) {
	conc := runtime.NumCPU()
	var wg sync.WaitGroup
	wg.Add(conc)
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
		subHeap.data = make([]int64, subHeap.len)
		copy(subHeap.data, arr[start:end])
		subHeaps = append(subHeaps, &subHeap)
		go BuildOne(&subHeap, &wg)
	}
	wg.Wait()
	var i int = 0
	for {
		selected, existed := SelectOne(subHeaps)
		if existed {
			arr[i] = selected
			i++
		} else {
			break
		}
	}
}

type subHeap struct {
	data []int64
	len  int
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
	for n := s.len/2 - 1; n >= 0; n-- {
		s.adjust(n)
	}
	// sort.Slice(s.data, func(x, y int) bool { return s.data[x] < s.data[y] })
	wg.Done()
}

func SelectOne(arrs []*subHeap) (selected int64, existed bool) {
	si := -1
	for i, s := range arrs {
		if s.len == 0 {
			continue
		}
		if !existed || selected > s.data[0] {
			selected = s.data[0]
			si = i
			existed = true
		}
	}
	if si >= 0 {
		arrs[si].data[0], arrs[si].data[arrs[si].len-1] = arrs[si].data[arrs[si].len-1], arrs[si].data[0]
		arrs[si].len--
		if arrs[si].len == 0 {
			// remove
			arrs[si], arrs[len(arrs)-1] = arrs[si], arrs[len(arrs)-1]
		} else {
			arrs[si].adjust(0)
		}
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
			arr[c].data[0] > arr[c+1].data[0] {
			c++
		}
		// 没有子节点 || 满足最小堆 || len = 0为已删除的节点
		if c >= total || arr[c].len == 0 || arr[parent].data[0] < arr[c].data[0] {
			break
		}
		arr[parent], arr[c] = arr[c], arr[parent]
		parent = c
	}
}

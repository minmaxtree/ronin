package skiplist

import (
    "fmt"
    "testing"
    "strconv"
)

func TestBasics(t *testing.T) {
    list := New()
    for i := 0; i < 10; i++ {
        list.Insert(strconv.Itoa(i), float64(i))
    }

    for i := 20; i > 10; i-- {
        list.Insert(strconv.Itoa(i), float64(i))
    }

    for i := 5; i < 15; i++ {
        err := list.Delete(strconv.Itoa(i), float64(i))
        if err != nil {
            fmt.Println("i is:", i)
            fmt.Println(err)
        }
    }
    list.debugPrint()

    for i := 0; i < 20; i++ {
        index, err := list.Search(strconv.Itoa(i), float64(i))
        if err == nil {
            fmt.Printf("i is %d, index is %d\n", i, index)
        }
    }

    for i := 0; i < 20; i++ {
        key, value, err := list.Lookup(i)
        if err == nil {
            fmt.Printf("i is %d, key is %s, value is %f\n", i, key, value)
        }
    }

    start := 3
    stop := 8
    keyValueList, err := list.LookupRange(start, stop)
    if err == nil {
        for _, keyValue := range keyValueList {
            fmt.Printf("keyValue.key is %s, keyValue.value is %f\n", keyValue.Key, keyValue.Value)
        }
    }

    count := list.Count(3, 17)
    fmt.Println("count is:", count)
}

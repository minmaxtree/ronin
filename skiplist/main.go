package skiplist

import (
    "errors"
    "math/rand"
    "time"
    "fmt"
)

type Node struct {
    key string
    value float64
    left *Node
    right *Node
    upper *Node
    lower *Node
    step int  // distance to left node
}

type List struct {
    head *Node
    levels int
    r *rand.Rand
}

func New() *List {
    s := rand.NewSource(time.Now().UnixNano())
    r := rand.New(s)

    list := new(List)
    list.head = nil
    list.levels = 0
    list.r = r

    return list
}

func (node *Node)print() {
    if node == nil {
        fmt.Println("<nil>")
        return
    }
    fmt.Printf("key is %s, value is %f, lower is %p\n", node.key, node.value, node.lower)
}

func (list *List)print() {
    if list.head == nil {
        fmt.Println("[empty]")
        return
    }

    ptr := list.head
    for ptr.lower != nil {
        ptr = ptr.lower
    }

    i := 0
    fmt.Println("list levels:", list.levels)
    for ; ptr != nil; ptr = ptr.right {
        fmt.Printf("[%d] %s: %f\n", i, ptr.key, ptr.value)
        i++
    }
}

func (list *List)debugPrint() {
    if list.head == nil {
        fmt.Println("[empty]")
        return
    }

    fmt.Println("==========")
    fmt.Println("list levels is:", list.levels)
    for head := list.head; head != nil; head = head.lower {
        i := 0
        for ptr := head; ptr != nil; ptr = ptr.right {
            fmt.Printf("[%d] %s: %f (+%d) <@[%p], lower[%p], right[%p]> | ", i, ptr.key, ptr.value, ptr.step,
                    ptr, ptr.lower, ptr.right)
            if ptr.right != nil {
                i += ptr.right.step
            }
        }
        fmt.Println("")
    }
    fmt.Println("==========")
}

func (list *List)Insert(key string, value float64) {
    fmt.Printf("[INSERT] key is %s, value is %f\n", key, value)
    if list.head == nil {
        head := new(Node)
        head.key = key
        head.value = value

        head.left = nil
        head.right = nil
        head.upper = nil
        head.lower = nil
        head.step = 0

        list.head = head
        list.levels = 1
        return
    }

    if list.head.value > value {
        var upper *Node = nil
        for ptr := list.head; ptr != nil; ptr = ptr.lower {
            node := new(Node)
            node.key = key
            node.value = value
            node.left = nil
            node.right = ptr
            node.lower = nil
            node.upper = upper
            node.step = 0

            if node.upper != nil {
                node.upper.lower = node
            }

            ptr.left = node
            ptr.step = 1

            upper = node
        }
        list.head = list.head.left

        return
    }

    ptr := list.head
    distances := make([]int, list.levels)
    insertPositions := make([]*Node, list.levels)

    list.debugPrint()
    for i := list.levels - 1; i >= 0; i-- {
        for ptr.right != nil && ptr.right.value < value {
            ptr = ptr.right
            distances[i] += ptr.step
        }
        insertPositions[i] = ptr
        ptr = ptr.lower
    }

    for _, position := range insertPositions {
        if position.right != nil {
            position.right.step++
        }
    }

    var lowerNode *Node = nil
    for i, position := range insertPositions {
        node := new(Node)
        node.value = value
        node.key = key
        node.left = position
        node.right = position.right

        node.left.right = node
        if node.right != nil {
            node.right.left = node
        }

        node.lower = lowerNode
        node.upper = nil

        if i == 0 {
            node.step = 1
        } else {
            node.step = lowerNode.step + distances[i-1]
        }

        if node.right != nil {
            node.right.step -= node.step
        }

        if lowerNode != nil {
            lowerNode.upper = node
        }

        lowerNode = node

        if list.r.Float64() < 0.5 {
            return
        }
    }

    step := lowerNode.step + distances[list.levels - 1]
    for {
        node := new(Node)
        head := new(Node)

        head.key = list.head.key
        head.value = list.head.value
        head.lower = list.head
        head.upper = nil
        head.right = node
        head.left = nil
        head.step = 0
        list.head.upper = head

        list.head = head
        list.levels++

        node.key = lowerNode.key
        node.value = lowerNode.value
        node.left = head
        node.right = nil
        node.upper = nil
        node.lower = lowerNode
        node.step = step

        lowerNode = node
        if lowerNode != nil {
            lowerNode.upper = node
        }

        if list.r.Float64() < 0.5 {
            return
        }
    }
}

func (list *List)Search(key string, value float64) (index int, err error) {
    ptr := list.head
    for {
        if ptr.key == key {
            return index, nil
        } else if ptr.right != nil && ptr.right.value <= value {
            ptr = ptr.right
            index += ptr.step
        } else if ptr.lower != nil {
            ptr = ptr.lower
        } else {
            return 0, errors.New("Key Not Found")
        }
    }
}

func (list *List)Delete(key string, value float64) error {
    if list.head.key == key {
        list.head = list.head.right
        for ptr := list.head; ptr != nil; ptr = ptr.lower {
            ptr.left = nil
            ptr.step = 0
        }
        return nil
    }

    ptr := list.head
    stepAffectedNodes := []*Node {}
    for {
        fmt.Printf("[DELETE] ptr.key is %s, key is %s, value is %f, ptr.lower is %p\n",
                ptr.key, key, value, ptr.lower)
        if ptr.right != nil {
            fmt.Printf("         ptr.right.value is %f\n", ptr.right.value)
        }
        if ptr.key == key {
            fmt.Println("ptr.upper is:", ptr.upper)
            for ptr != nil {
                ptr.left.right = ptr.right
                if ptr.right != nil {
                    ptr.right.left = ptr.left
                    ptr.right.step = ptr.right.step + ptr.step - 1
                }

                ptr = ptr.lower
            }

            for _, node := range stepAffectedNodes {
                fmt.Printf("[...] node.step is %d, node.value is %f, value is %f\n", node.step,
                    node.value, value)
                node.step--
            }

            if list.head.right == nil {
                list.head = list.head.lower
            }

            for list.head.lower != nil && list.head.right == nil {
                list.head = list.head.lower
                list.levels--
            }
            list.head.upper = nil

            return nil
        } else if ptr.right != nil && ptr.right.value <= value {
            ptr = ptr.right
        } else if ptr.lower != nil {
            if ptr.right != nil {
                stepAffectedNodes = append(stepAffectedNodes, ptr.right)
            }
            ptr = ptr.lower
        } else {
            return errors.New("Key Not Found")
        }
    }
}

func (list *List)Lookup(index int) (key string, value float64, err error) {
    ptr := list.head
    cur_index := 0
    for {
        if cur_index == index {
            return ptr.key, ptr.value, nil
        } else if ptr.right != nil && cur_index + ptr.right.step <= index {
            ptr = ptr.right
            cur_index += ptr.step
        } else if ptr.lower != nil {
            ptr = ptr.lower
        } else {
            return "", 0, errors.New("Index Out Of Range")
        }
    }
}

type KeyValue struct {
    Key string
    Value float64
}

// start and stop both included
func (list *List)LookupRange(start int, stop int) (keyValueList []*KeyValue, err error) {
    ptr := list.head
    cur_index := 0
    for {
        if cur_index == start {
            for ptr.lower != nil {
                ptr = ptr.lower
            }
            keyValueList = []*KeyValue {}
            for i := start; i <= stop; i++ {
                keyValue := new(KeyValue)
                keyValue.Key = ptr.key
                keyValue.Value = ptr.value
                keyValueList = append(keyValueList, keyValue)

                ptr = ptr.right
                if ptr == nil {
                    break
                }
            }
            return keyValueList, nil
        } else if ptr.right != nil && cur_index + ptr.right.step <= start {
            ptr = ptr.right
            cur_index += ptr.step
        } else if ptr.lower != nil {
            ptr = ptr.lower
        } else {
            return []*KeyValue {}, errors.New("Index Out of Range")
        }
    }
}

// complexity O(log(n))
func (list *List)Count(min float64, max float64) int {
    ptr := list.head

    var minKey string
    var minValue float64
    var maxKey string
    var maxValue float64

    for {
        fmt.Printf("ptr.value is %f, min is %f\n", ptr.value, min)
        if ptr.value >= min {
            minKey = ptr.key
            minValue = ptr.value
            break
        } else if ptr.right != nil {
            ptr = ptr.right
        } else if ptr.lower != nil {
            ptr = ptr.lower
        } else {
            return 0
        }
    }

    for {
        if ptr.right != nil && ptr.right.value > max {
            maxKey = ptr.key
            maxValue = ptr.value
            break
        } else if ptr.right != nil && ptr.right.value <= max {
            ptr = ptr.right
        } else if ptr.lower != nil {
            ptr = ptr.lower
        } else {
            maxKey = ptr.key
            maxValue = ptr.value
            break
        }
    }

    minIndex, _ := list.Search(minKey, minValue)
    maxIndex, _ := list.Search(maxKey, maxValue)
    return maxIndex - minIndex + 1
}

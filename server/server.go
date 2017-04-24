package main

import (
    "fmt"
    "strconv"
    "net"
    "bufio"
    "sync"
    "log"
    "os"

    "container/list"

    "ronin"
    "ronin/skiplist"
)

func wrongTypeError() []byte {
    return []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

func wrongNumberError(command string) []byte {
    s := fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", command)
    return []byte(s)
}

func emptyArray() []byte {
    return []byte("*0\r\n")
}

func stringArray(array []string) []byte {
    str := fmt.Sprintf("*%d\r\n", len(array))
    for _, s := range array {
        str += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
    }
    return []byte(str)
}

func main() {
    port := 33333
    service := ":" + strconv.Itoa(port)

    ln, err := net.Listen("tcp", service)
    check(err)

    mutex := &sync.Mutex {}

    hashTable := map[string]interface{} {}

    // channels := map[string][]net.Conn {}
    channels := map[string]map[net.Conn]bool {}

    for {
        conn, err := ln.Accept()
        check(err)

        go func() {
            var chanNames []string

            reader := bufio.NewReader(conn)

        MainLoop:
            for {
                vn_msg_cr, err := reader.ReadString('\r')
                if err != nil {
                    conn.Close()
                    if len(chanNames) != 0 {
                        for _, name := range chanNames {
                            channel := channels[name]
                            delete(channel, conn)
                            if len(channel) == 0 {
                                delete(channels, name)
                            }
                        }
                    }
                    return
                }
                _, err = reader.ReadByte()
                if err != nil {
                    conn.Close()
                    return
                }
                vn, err := strconv.Atoi(vn_msg_cr[1:(len(vn_msg_cr)-1)])
                check(err)
                fmt.Printf("vn is %d\n", vn)

                coms := []string {}

                for i := 0; i < vn; i++ {
                    _, err := reader.ReadString('\n') // skip length, not needed
                    if err != nil {
                        conn.Close()
                        return
                    }
                    v1_crlf, err := reader.ReadString('\n')
                    if err != nil {
                        conn.Close()
                        return
                    }
                    v1 := v1_crlf[:len(v1_crlf)-2]
                    fmt.Printf("v1 is %s\n", v1)
                    coms = append(coms, v1)
                }

                switch coms[0] {
                case "set":
                    if len(coms) != 3 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        mutex.Lock()
                        hashTable[coms[1]] = coms[2]
                        mutex.Unlock()
                        conn.Write([]byte("+OK\r\n"))
                    }
                case "get":
                    if len(coms) != 2 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        mutex.Lock()
                        if (errorWhenNot(conn, hashTable[coms[1]], "")) {
                            valx := hashTable[coms[1]]
                            if valx == nil {
                                conn.Write([]byte("$-1\r\n"))
                            } else {
                                val := valx.(string)
                                fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
                            }
                        }
                        mutex.Unlock()
                    }
                case "incr":
                    if len(coms) != 2 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        mutex.Lock()
                        val, err := strconv.Atoi(hashTable[coms[1]].(string))
                        if err != nil {
                            conn.Write([]byte("-ERR value is not an integer\r\n"))
                        } else {
                            new_val := strconv.Itoa(val + 1)
                            hashTable[coms[1]] = new_val
                            conn.Write([]byte("+OK\r\n"))
                        }
                        mutex.Unlock()
                    }
                case "decr":
                    if len(coms) != 2 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        mutex.Lock()
                        val, err := strconv.Atoi(hashTable[coms[1]].(string))
                        if err != nil {
                            conn.Write([]byte("-ERR value is not an integer\r\n"))
                        } else {
                            new_val := strconv.Itoa(val - 1)
                            hashTable[coms[1]] = new_val
                            mutex.Unlock()
                            conn.Write([]byte("+OK\r\n"))
                        }
                    }
                case "lpush":
                    if len(coms) < 3 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        listName := coms[1]
                        valsToPush := coms[2:]
                        mutex.Lock()
                        var listVal *list.List
                        switch hashTable[listName].(type) {
                        case nil:
                            listVal = list.New()
                        case *list.List:
                            listVal = hashTable[listName].(*list.List)
                        default:
                            conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
                            mutex.Unlock()
                            continue
                        }
                        for _, v := range(valsToPush) {
                            listVal.PushFront(v)
                        }
                        hashTable[listName] = listVal
                        mutex.Unlock()
                        fmt.Fprintf(conn, ":%d\r\n", listVal.Len())
                    }
                case "rpush":
                    if len(coms) < 3 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        listName := coms[1]
                        valsToPush := coms[2:]
                        mutex.Lock()
                        var listVal *list.List
                        switch hashTable[listName].(type) {
                        case nil:
                            listVal = list.New()
                        case *list.List:
                            listVal = hashTable[listName].(*list.List)
                        default:
                            conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
                            mutex.Unlock()
                            continue
                        }
                        for _, v := range(valsToPush) {
                            listVal.PushBack(v)
                        }
                        hashTable[listName] = listVal
                        mutex.Unlock()
                        fmt.Fprintf(conn, ":%d\r\n", listVal.Len())
                    }
                case "lpushx":
                    if len(coms) < 3 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        listName := coms[1]
                        valsToPush := coms[2:]
                        mutex.Lock()
                        var listVal *list.List
                        switch hashTable[listName].(type) {
                        case nil:
                            conn.Write([]byte(":0\r\n"))
                            mutex.Unlock()
                            continue;
                        case *list.List:
                            listVal = hashTable[listName].(*list.List)
                        default:
                            conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
                            mutex.Unlock()
                            continue
                        }
                        for _, v := range(valsToPush) {
                            listVal.PushFront(v)
                        }
                        hashTable[listName] = listVal
                        mutex.Unlock()
                        fmt.Fprintf(conn, ":%d\r\n", listVal.Len())
                    }
                case "rpushx":
                    if len(coms) < 3 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        listName := coms[1]
                        valsToPush := coms[2:]
                        mutex.Lock()
                        var listVal *list.List
                        switch hashTable[listName].(type) {
                        case nil:
                            conn.Write([]byte(":0\r\n"))
                            mutex.Unlock()
                            continue;
                        case *list.List:
                            listVal = hashTable[listName].(*list.List)
                        default:
                            conn.Write([]byte("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
                            mutex.Unlock()
                            continue
                        }
                        for _, v := range(valsToPush) {
                            listVal.PushBack(v)
                        }
                        hashTable[listName] = listVal
                        mutex.Unlock()
                        fmt.Fprintf(conn, ":%d\r\n", listVal.Len())
                    }
                case "lpop":
                    if len(coms) != 2 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        listName := coms[1]
                        mutex.Lock()
                        switch hashTable[listName].(type) {
                        case *list.List:
                            l := hashTable[listName].(*list.List)
                            val := l.Remove(l.Front()).(string)
                            if l.Len() == 0 {
                                hashTable[listName] = nil
                            }
                            fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
                        case nil:
                            fmt.Fprintf(conn, "$-1\r\n")  // null string
                        default:
                            fmt.Fprintf(conn, "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                        }
                        mutex.Unlock()
                    }
                case "rpop":
                    if len(coms) != 2 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        listName := coms[1]
                        mutex.Lock()
                        switch hashTable[listName].(type) {
                        case *list.List:
                            l := hashTable[listName].(*list.List)
                            val := l.Remove(l.Back()).(string)
                            if l.Len() == 0 {
                                hashTable[listName] = nil
                            }
                            fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
                        case nil:
                            fmt.Fprintf(conn, "$-1\r\n")  // null string
                        default:
                            fmt.Fprintf(conn, "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                        }
                        mutex.Unlock()
                    }
                case "del":
                    if len(coms) < 2 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        mutex.Lock()
                        count := 0
                        for _, name := range(coms[1:]) {
                            if hashTable[name] != nil {
                                count += 1
                                hashTable[name] = nil
                            }
                        }
                        mutex.Unlock()
                        fmt.Fprintf(conn, ":%d\r\n", count)
                    }
                case "type":
                    if len(coms) != 2 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        mutex.Lock()
                        var typ string
                        switch hashTable[coms[1]].(type) {
                        case nil:
                            typ = "none"
                        case string:
                            typ = "string"
                        case *list.List:
                            typ = "list"
                        case map[string]bool:
                            typ = "set"
                        case map[string]string:
                            typ = "hash"
                        default:
                            typ = "tbd"
                        }
                        mutex.Unlock()
                        fmt.Fprintf(conn, "+%s\r\n", typ)
                    }
                case "lrange":
                    if len(coms) != 4 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        listName := coms[1]
                        start, err := strconv.Atoi(coms[2])
                        if (err != nil) {
                            fmt.Fprintf(conn, "-ERR value is not an integer")
                            continue
                        }
                        end, err := strconv.Atoi(coms[3])
                        if (err != nil) {
                            fmt.Fprintf(conn, "-ERR value is not an integer")
                            continue
                        }
                        mutex.Lock()
                        switch hashTable[listName].(type) {
                        case nil:
                            conn.Write([]byte("*0\r\n"))
                        case *list.List:
                            lis := hashTable[listName].(*list.List)
                            message_p2 := ""
                            e := lis.Front();
                            for i := 0; i < start; i++ {
                                e = e.Next()
                            }
                            if end < 0 {
                                end += lis.Len()
                            }
                            count := 0
                            for i := start; i <= end && e != nil; i++ {
                                v := e.Value.(string)
                                message_p2 += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
                                count += 1
                                e = e.Next()
                            }
                            message_p1 := fmt.Sprintf("*%d\r\n", count)
                            message := message_p1 + message_p2
                            conn.Write([]byte(message))
                        default:
                            fmt.Fprintf(conn, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                        }
                        mutex.Unlock()
                    }
                // set operations
                case "sadd":
                    if len(coms) < 3 {
                        fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", coms[0])
                    } else {
                        mutex.Lock()
                        members := coms[2:]
                        switch hashTable[coms[1]].(type) {
                        case nil:
                            hashTable[coms[1]] = map[string]bool{}
                        case map[string]bool:
                            break
                        default:
                            mutex.Unlock()
                            conn.Write(wrongTypeError())
                            continue
                        }
                        key := hashTable[coms[1]].(map[string]bool)
                        ret := 0
                        for _, member := range members {
                            if key[member] == false {
                                key[member] = true
                                ret += 1
                            }
                        }
                        mutex.Unlock()
                        fmt.Fprintf(conn, ":%d\r\n", ret)
                    }
                case "smembers":
                    if len(coms) != 2 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        mutex.Lock()
                        switch hashTable[coms[1]].(type) {
                        case nil:
                            conn.Write(emptyArray())
                        case map[string]bool:
                            set := hashTable[coms[1]].(map[string]bool)
                            members := make([]string, len(set))
                            i := 0
                            for member := range set {
                                members[i] = member
                                i++
                            }
                            conn.Write(stringArray(members))
                        default:
                            conn.Write(wrongTypeError())
                        }
                        mutex.Unlock()
                    }
                case "srem":
                    if len(coms) < 3 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        mutex.Lock()
                        members := coms[2:]
                        count := 0
                        if (errorWhenNot(conn, hashTable[coms[1]], map[string]bool{})) {
                            set := hashTable[coms[1]].(map[string]bool)
                            for _, member := range members {
                                if set[member] {
                                    delete(set, member)
                                    count++
                                }
                            }
                            conn.Write(integer(count))
                        }
                        mutex.Unlock()
                    }
                case "scard":
                    if len(coms) != 2 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        mutex.Lock()
                        if (errorWhenNot(conn, hashTable[coms[1]], map[string]bool{})) {
                            conn.Write(integer(len(hashTable[coms[1]].(map[string]bool))))
                        }
                        mutex.Unlock()
                    }
                // sorted-set operations
                case "zadd":
                    if len(coms) < 4 || len(coms) % 2 != 0 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        key := coms[1]
                        pairs := map[string]float64 {}
                        for i := 2; i < len(coms); i += 2 {
                            score, err := strconv.ParseFloat(coms[i], 64)
                            if err != nil {
                                conn.Write(errorString("ERR value is not a valid float"))
                                continue MainLoop
                            }
                            member := coms[i+1]
                            pairs[member] = score
                        }

                        mutex.Lock()
                        var sortedSet *ronin.SortedSet = nil
                        switch hashTable[key].(type) {
                        case nil:
                            sortedSet = new(ronin.SortedSet)
                            sortedSet.List = skiplist.New()
                            sortedSet.KVMap = map[string]float64 {}

                            hashTable[key] = sortedSet
                        case *ronin.SortedSet:
                            sortedSet = hashTable[key].(*ronin.SortedSet)
                        default:
                            conn.Write(wrongTypeError())
                        }
                        if sortedSet != nil {
                            n := 0
                            for member, score := range pairs {
                                if _, ok := sortedSet.KVMap[member]; !ok {
                                    sortedSet.KVMap[member] = score
                                    sortedSet.List.Insert(member, score)
                                    n++
                                }
                            }
                            conn.Write(integer(n))
                        }
                        mutex.Unlock()
                    }
                case "zrank":  // O(log(n))
                    if len(coms) != 3 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        key := coms[1]
                        member := coms[2]

                        mutex.Lock()
                        switch hashTable[key].(type) {
                        case nil:
                            conn.Write(emptyArray())
                        case *ronin.SortedSet:
                            sortedSet := hashTable[key].(*ronin.SortedSet)
                            score := sortedSet.KVMap[member]
                            rank, err := sortedSet.List.Search(member, score)
                            if err != nil {
                                conn.Write(emptyArray())
                            } else {
                                conn.Write(integer(rank))
                            }
                        default:
                            conn.Write(wrongTypeError())
                        }
                        mutex.Unlock()
                    }
                case "zrange":
                    if len(coms) != 4 && len(coms) != 5 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        key := coms[1]
                        start, _ := strconv.Atoi(coms[2])
                        stop, _ := strconv.Atoi(coms[3])
                        var withScores bool
                        if len(coms) == 5 {
                            withScores = true
                        } else {
                            withScores = false
                        }

                        switch hashTable[key].(type) {
                        case nil:
                            conn.Write(emptyArray())
                        case *ronin.SortedSet:
                            mutex.Lock()
                            sortedSet := hashTable[key].(*ronin.SortedSet)
                            keyValueList, err := sortedSet.List.LookupRange(start, stop)
                            if err != nil {
                                conn.Write(emptyArray())
                            } else {
                                vals := []string {}
                                if withScores {
                                    for _, keyValue := range keyValueList {
                                        vals = append(vals, keyValue.Key)
                                        vals = append(vals, fmt.Sprintf("%f", keyValue.Value))
                                    }
                                } else {
                                    for _, keyValue := range keyValueList {
                                        vals = append(vals, keyValue.Key)
                                    }
                                }
                                conn.Write(stringArray(vals))
                            }
                            mutex.Unlock()
                        default:
                            conn.Write(wrongTypeError())
                        }
                    }
                case "zrem":
                    if len(coms) < 3 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        key := coms[1]
                        members := coms[2:]

                        mutex.Lock()
                        switch hashTable[key].(type) {
                        case nil:
                            conn.Write(integer(0))
                        case *ronin.SortedSet:
                            sortedSet := hashTable[key].(*ronin.SortedSet)

                            n := 0
                            for _, member := range members {
                                value := sortedSet.KVMap[member]
                                err := sortedSet.List.Delete(member, value)
                                if err != nil {
                                } else {
                                    n++
                                }
                            }
                            conn.Write(integer(n))
                        }
                        mutex.Unlock()
                    }
                case "zcount":  // O(log(n))
                    if len(coms) != 4 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        key := coms[1]
                        min, err := strconv.ParseFloat(coms[2], 64)
                        if err != nil {
                            conn.Write(errorString("min or max is not a float"))
                            continue
                        }
                        max, err := strconv.ParseFloat(coms[3], 64)
                        if err != nil {
                            conn.Write(errorString("min or max is not a float"))
                            continue
                        }

                        mutex.Lock()
                        switch hashTable[key].(type) {
                        case nil:
                            conn.Write(NullBulkString)
                        case *ronin.SortedSet:
                            sortedSet := hashTable[key].(*ronin.SortedSet)
                            count := sortedSet.List.Count(min, max)
                            conn.Write(integer(count))
                        default:
                            conn.Write(wrongTypeError())
                        }
                        mutex.Unlock()
                    }
                case "zscore":  // O(1)
                    if len(coms) != 3 {
                        conn.Write(wrongNumberError(coms[1]))
                    } else {
                        key := coms[1]
                        member := coms[2]

                        mutex.Lock()
                        switch hashTable[key].(type) {
                        case nil:
                            conn.Write(NullBulkString)
                        case *ronin.SortedSet:
                            sortedSet := hashTable[key].(*ronin.SortedSet)
                            value, ok := sortedSet.KVMap[member]
                            fmt.Println("[zscore] value is:", value)
                            if ok {
                                conn.Write(bulkString(fmt.Sprintf("%f", value)))
                            } else {
                                conn.Write(NullBulkString)
                            }
                        default:
                            conn.Write(wrongTypeError())
                        }
                        mutex.Unlock()
                    }

                // pub/sub
                case "subscribe":
                    if len(coms) < 2 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        chanNames = coms[1:]
                        resp := ""
                        for i, name := range chanNames {
                            // channels[name] = append(channels[name], conn)
                            if channels[name] == nil {
                                channels[name] = map[net.Conn]bool {}
                            }
                            channels[name][conn] = true
                            resp += fmt.Sprintf("*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n:%d\r\n",
                                len("subscribe"), "subscribe", len(name), name, i + 1)
                        }
                        conn.Write([]byte(resp))
                    }
                case "publish":
                    if len(coms) != 3 {
                        conn.Write(wrongNumberError(coms[0]))
                    } else {
                        chanName := coms[1]
                        message := coms[2]
                        subscribers := channels[chanName]
                        conn.Write(integer(len(subscribers)))

                        for subscriber := range subscribers {
                            subscriber.Write(stringArray([]string { "message", chanName, message }))
                        }
                    }
                default:
                    conn.Write([]byte("-ERR"))
                    fmt.Fprintf(conn, "-ERR unknown command '%s'\r\n", coms[0])
                }
            }
        }()
    }
}

func errorWhenNot(conn net.Conn, value interface {}, match interface {}) bool {
    if value == nil {
        conn.Write(emptyArray())
        return false
    }

    switch match.(type) {
    case string:
        switch value.(type) {
        case string:
            return true
        }
    case *list.List:
        switch value.(type) {
        case *list.List:
            return true
        }
    case map[string]bool:
        switch value.(type) {
        case map[string]bool:
            return true
        }
    case map[string]string:
        switch value.(type) {
        case map[string]string:
            return true
        }
    }

    conn.Write(wrongTypeError())
    return false
}

func integer(value int) []byte {
    return []byte(fmt.Sprintf(":%d\r\n", value))
}

func nullBulkString() []byte {
    return []byte("$-1\r\n")
}

var NullBulkString []byte = []byte("$-1\r\n")

func bulkString(value string) []byte {
    return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
}

func errorString(value string) []byte {
    return []byte(fmt.Sprintf("-%s\r\n", value))
}

func check(err error) {
    if err != nil {
        log.Println(err)
        os.Exit(-1)
    }
}

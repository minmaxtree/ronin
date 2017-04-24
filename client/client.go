package main

import (
    "fmt"
    "net"
    "bufio"
    "strconv"
    "os"
    "strings"

    "ronin"
)

func shell() {
    port := 33333
    service := "127.0.0.1:" + strconv.Itoa(port)

    conn, err := net.Dial("tcp", service)
    check(err)
    r := bufio.NewReader(os.Stdin)
    connReader := bufio.NewReader(conn)
    for {
        os.Stdout.Write([]byte(service + "> "))
        c, err := r.ReadString('\n')
        if err != nil {
            conn.Close()
            return
        }
        cc := strings.Fields(c[:len(c)-1])
        if len(cc) == 0 {
            continue
        }
        cm := "*" + strconv.Itoa(len(cc)) + "\r\n"
        for _, cp := range cc {
            cm += "$" + strconv.Itoa(len(cp)) + "\r\n" + cp + "\r\n"
        }
        fmt.Fprintf(conn, cm)

        if cc[0] == "subscribe" {
            for {
                rec, err := ronin.ReadMessage(connReader)
                check(err)
                printMessage(rec)
            }
        }

        rec, err := ronin.ReadMessage(connReader)
        printMessage(rec)
    }
}

func printMessage(message interface{}) {
    switch message.(type) {
    case string:
        fmt.Printf("%s\n", message.(string))
    case ronin.ErrorString:
        fmt.Printf("(error) %s\n", message.(ronin.ErrorString).Value)
    case int:
        fmt.Printf("(integer) %d\n", message.(int))
    case nil:
        fmt.Printf("(nil)\n")
    case []interface{}:
        slice := message.([]interface{})
        if len(slice) == 0 {
            fmt.Println("(empty list or set)")
        } else {
            for i, v := range(slice) {
                fmt.Printf("%d)", i + 1)
                printMessage(v)
            }
        }
    default:
        fmt.Printf("tbd\n")
    }
}

func main() {
    shell()
}

func check(err error) {
    if err != nil {
        panic(err)
    }
}

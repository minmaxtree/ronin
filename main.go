package ronin

import (
    "bufio"
    "strconv"
    "ronin/skiplist"
)

type ErrorString struct {
    Value string
}

func ReadMessage(reader *bufio.Reader) (interface{}, error) {
    tb, err := reader.ReadByte()
    if err != nil {
        return nil, err
    }
    switch tb {
    case '*':  // array
        vns, err := reader.ReadString('\n')
        if err != nil {
            return nil, err
        }
        vn, _ := strconv.Atoi(vns[:len(vns)-2])

        if vn == -1 { // null array
            return make([]interface{}, 0), nil
        } else {
            res := make([]interface{}, vn)

            for i := 0; i < vn; i++ {
                v, err := ReadMessage(reader)
                if err != nil {
                    return nil, err
                }
                res[i] = v
            }
            return res, nil
        }
    case '+':  // simple string
        sx, err := reader.ReadString('\n')
        if err != nil {
            return nil, err
        }
        return sx[:len(sx)-2], nil
    case '$':  // bulk string
        lnx, err := reader.ReadString('\n')
        if err != nil {
            return nil, err
        }
        ln, _ := strconv.Atoi(lnx[:len(lnx)-2])
        if ln == -1 { // null string
            return nil, nil
        } else {
            sx, err := reader.ReadString('\n')
            if err != nil {
                return nil, err
            }
            return sx[:len(sx)-2], nil
        }
    case '-':  // error
        sx, err := reader.ReadString('\n')
        if err != nil {
            return nil, err
        }
        ret := ErrorString { Value: sx[:len(sx)-2] }
        return ret, nil
    case ':':  // integer
        ix, err := reader.ReadString('\n')
        if err != nil {
            return nil, err
        }
        i, _ := strconv.Atoi(ix[:len(ix)-2])
        return i, nil
    default:
        return nil, nil
    }
}

type SortedSet struct {
    KVMap map[string]float64
    List *skiplist.List
}

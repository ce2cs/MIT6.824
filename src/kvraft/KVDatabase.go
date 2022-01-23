package kvraft

type KVDatabase struct {
	Data map[string]string
}

func (kvd *KVDatabase) get(key string) (string, Err) {
	value, prs := kvd.Data[key]
	if !prs {
		return "", ErrNoKey
	}
	return value, OK
}

func (kvd *KVDatabase) append(key string, value string) Err {
	_, prs := kvd.Data[key]
	if !prs {
		return kvd.put(key, value)
	}
	kvd.Data[key] += value
	return OK
}

func (kvd *KVDatabase) put(key string, value string) Err {
	kvd.Data[key] = value
	return OK
}

func NewKVDB() *KVDatabase {
	kvd := KVDatabase{}
	kvd.Data = make(map[string]string)
	return &kvd
}

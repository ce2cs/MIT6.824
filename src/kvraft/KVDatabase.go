package kvraft

type KVDatabase struct {
	data map[string]string
}

func (kvd *KVDatabase) get(key string) (string, Err) {
	value, prs := kvd.data[key]
	if !prs {
		return "", ErrNoKey
	}
	return value, OK
}

func (kvd *KVDatabase) append(key string, value string) Err {
	_, prs := kvd.data[key]
	if !prs {
		return kvd.put(key, value)
	}
	kvd.data[key] += value
	return OK
}

func (kvd *KVDatabase) put(key string, value string) Err {
	kvd.data[key] = value
	return OK
}

func NewKVDB() *KVDatabase {
	kvd := KVDatabase{}
	kvd.data = make(map[string]string)
	return &kvd
}

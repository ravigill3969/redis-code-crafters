package handlers

import "fmt"

var redisStreams = map[string][]struct {
	ID     string
	Fields map[string]string
}{}

func XADD(cmd []interface{}) string {

	// cmd[0] = "XADD"
	// cmd[1] = stream key
	// cmd[2] = entry ID
	// cmd[3:] = field-value pairs

	fmt.Println(cmd...)
	rstream := fmt.Sprintf("%v", cmd[1])
	id := fmt.Sprintf("%v", cmd[2])

	fields := map[string]string{}

	for i := 3; i < len(cmd); i += 2 {
		key := fmt.Sprintf("%v", cmd[i])
		if i+1 < len(cmd) {
			fields[key] = fmt.Sprintf("%v", cmd[i+1])
		}
	}

	entry := struct {
		ID     string
		Fields map[string]string
	}{
		ID:     id,
		Fields: fields,
	}

	redisStreams[rstream] = append(redisStreams[rstream], entry)

	return id

}

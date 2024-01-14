<center>
    <img src="./images/fairymq.png" width="200" />
</center>

*****************
fairyMQ GO Native Client Module

### Install
```
go get github.com/fairymq/fairymq-go
```
OR
```
go mod download github.com/fairymq/fairymq-go
```

## Using
``` 
	client := &fairymqgo.Client{
		Host:      "", // i.e 0.0.0.0
		PublicKey: "", // i.e example.public.pem
	}

	err := client.Enqueue([]byte("Hello world"))
	if err != nil {
		log.Fatalf(err.Error())
	}

	err = client.Enqueue([]byte("Hello again"))
	if err != nil {
		log.Fatalf(err.Error())
	}

	first, err := client.FirstIn() // Get first up in queue
	if err != nil {
		log.Fatalf(err.Error()) 
	}
	
	//.. do something with last message bytes
```
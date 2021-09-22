package throw

// OnError will panic if err is not nill
func OnError(err error) {
	if err != nil {
		panic(err)
	}
}

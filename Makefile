obu:
	@go build -o obu/bin/obu obu/main.go
	@obu/bin/obu

receiver:
	@go build -o receiver/bin/receiver ./receiver
	@receiver/bin/receiver

.PHONY: obu receiver

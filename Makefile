MAGIC_MODULES := \
	github.com/magic-lib/go-plat-utils \

gen-go-get-magic:
	@for mod in $(MAGIC_MODULES); do \
		echo "更新 $$mod@master"; \
		GOPROXY=direct go get -u $$mod@master; \
	done
	go mod tidy
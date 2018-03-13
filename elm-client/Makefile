YARN    := $(shell command -v yarn 2> /dev/null)
SRCS    := $(wildcard src/*)
TARGETS := dist/app.js dist/index.html dist/uikit.statser.min.css

.PHONY:	all build clean rebuild

all: build

build: $(TARGETS)

$(TARGETS): $(SRCS)
ifndef YARN
	npm install
	npm run build
else
	yarn
	yarn build
endif

clean:
	@rm -f $(TARGETS)

rebuild: clean build

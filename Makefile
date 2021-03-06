SRCS     := $(wildcard src/*)
INCLUDES := $(wildcard includes/*)

.PHONY:	all server client clean release start test rebuild

all: server client

client:
	$(MAKE) -C elm-client

server: $(SRCS) $(INCLUDES)
	rebar3 compile

release: all
	rebar3 release

start: all
	@./start.sh

test:
	rebar3 eunit

clean:
	@rm -rf _build
	$(MAKE) -C elm-client clean

rebuild: clean all

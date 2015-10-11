all: build

clean:
	rm -rf ./ocaml/_build

build:
	cd ocaml && ocamlbuild -j 0 -use-ocamlfind lsm.native

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

.PHONY: list build dep install image playground check test install_and_config

stor?=""
prefix?= "$(PWD)/projects"
release?= 0
dep?= 0
only?= "*"
image_name?= "dingodatabase/dingofs:unknown"
case?= "*"
os?= "rocky9"
ci?=0
image_type?=0

define help_msg
## list
Usage:
    make list

## build
Usage:
    make build only=TARGET dep=0/1 release=0/1 os=OS
Examples:
    make build only=test/* os=rocky9
    make build release=1


## dep
Usage:
    make dep

## install
Usage:
    make install prefix=PREFIX only=TARGET
Examples:
    make install prefix=/usr/local/dingofs only=etcd


## image
Usage:
    make image image_name=image_name os=OS image_type=TYPE # 0: skip build image, 1: build mds v1 image, 2: build mdsv2 image
Examples:
    make image image_name=dingodatabase/dingofs:test os=rocky9 image_type=1
endef
export help_msg

help:
	@echo "$$help_msg"

list:
	@bash build-scripts/build.sh --stor=fs --list

file_build:
	@bash build-scripts/file-build.sh --only=$(only) --dep=$(dep) --release=$(release) --os=$(os) --unit_tests=${unit_tests}

file_dep:
	@bash build-scripts/file-build.sh --only="" --dep=1

file_install:
	@bash build-scripts/file-install.sh --prefix=$(prefix) --only=$(only)

file_deploy_config:
	@bash build-scripts/file-deploy-config.sh --os=$(os)

file_image:
	@bash build-scripts/file-deploy-config.sh --os=$(os) --name=$(image_name) --type=${image_type}

playground:
	@bash build-scripts/playground.sh

check:
	@bash build-scripts/check.sh fs

test:
	@bash build-scripts/test.sh fs $(only)

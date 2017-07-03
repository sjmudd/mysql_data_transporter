package main

type Copier interface {
	Pre()
	Run()
	Post()
}

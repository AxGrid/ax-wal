module github.com/axgrid/ax-wal/axqcompat

go 1.23

require (
	github.com/axgrid/ax-wal v0.0.0-00010101000000-000000000000
	github.com/axgrid/axq v0.6.5
	github.com/golang/protobuf v1.5.4
)

require (
	github.com/axgrid/axtransform v0.0.0-20240321090434-11347af1e82e // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/go-sql-driver/mysql v1.7.0 // indirect
	github.com/golang/glog v1.2.0 // indirect
	github.com/google/readahead v0.0.0-20161222183148-eaceba169032 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	github.com/rs/zerolog v1.32.0 // indirect
	github.com/speps/go-hashids v2.0.0+incompatible // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/exp v0.0.0-20240222234643-814bf88cf225 // indirect
	golang.org/x/sys v0.28.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/kothar/go-backblaze.v0 v0.0.0-20210124194846-35409b867216 // indirect
	gorm.io/driver/mysql v1.5.4 // indirect
	gorm.io/gorm v1.25.7 // indirect
)

// Use the sibling working tree until ax-wal is published.
replace github.com/axgrid/ax-wal => ../

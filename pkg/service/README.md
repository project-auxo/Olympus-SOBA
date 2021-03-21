
Be sure to compile the .proto for the services, e.g.

`protoc --go_out=paths=source_relative:../../../gen/proto -I. echo.proto`


`protoc --proto_path=src --go_out=out --go_opt=paths=source_relative foo.proto bar/baz.proto`

the compiler will read input files foo.proto and bar/baz.proto from within the src directory, and write output files foo.pb.go and bar/baz.pb.go to the out directory. The compiler automatically creates nested output sub-directories if necessary, but will not create the output directory itself.



Write service, ensure import in service.go
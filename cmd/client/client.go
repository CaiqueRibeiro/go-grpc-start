package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/CaiqueRibeiro/fc2-grpc/pb"
	"google.golang.org/grpc"
)

func main() {

	connection, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to gRPC server: %v", err)
	}
	defer connection.Close()

	client := pb.NewUserServiceClient(connection)
	// AddUser(client)
	//AddUserVerbose(client)
	//AddUsers(client)
	AddUserStreamBoth(client)
}

func AddUser(client pb.UserServiceClient) {

	req := &pb.User{
		Id:    "0",
		Name:  "Caique",
		Email: "caique@gmail.com",
	}

	res, err := client.AddUser(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	fmt.Println(res)

}

func AddUserVerbose(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "Joao",
		Email: "joaozito.com",
	}

	responseStream, err := client.AddUserVerbose(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	for {
		stream, err := responseStream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("Could not receive message: %v", err)
		}

		fmt.Println("Status: ", stream.Status, " - ", stream.GetUser())
	}
}

func AddUsers(client pb.UserServiceClient) {
	reqs := []*pb.User{
		&pb.User{
			Id:    "1",
			Name:  "Caique",
			Email: "caique.teste@gmail.com",
		},
		&pb.User{
			Id:    "2",
			Name:  "Caique 2",
			Email: "caique2.teste@gmail.com",
		},
		&pb.User{
			Id:    "3",
			Name:  "Caique 3",
			Email: "caique3.teste@gmail.com",
		},
		&pb.User{
			Id:    "4",
			Name:  "Caique 4",
			Email: "caique4.teste@gmail.com",
		},
		&pb.User{
			Id:    "5",
			Name:  "Caique 5",
			Email: "caique5.teste@gmail.com",
		},
	}

	stream, err := client.AddUsers(context.Background())

	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	for _, req := range reqs {
		stream.Send(req)
		time.Sleep(time.Second * 2)
	}

	res, err := stream.CloseAndRecv()

	if err != nil {
		log.Fatalf("Error receiving request: %v", err)
	}

	fmt.Println(res)
}

func AddUserStreamBoth(client pb.UserServiceClient) {

	stream, err := client.AddUserStreamBoth(context.Background())

	if err != nil {
		log.Fatalf("Error creating request %v", err)
	}

	reqs := []*pb.User{
		&pb.User{
			Id:    "1",
			Name:  "Caique",
			Email: "caique.teste@gmail.com",
		},
		&pb.User{
			Id:    "2",
			Name:  "Caique 2",
			Email: "caique2.teste@gmail.com",
		},
		&pb.User{
			Id:    "3",
			Name:  "Caique 3",
			Email: "caique3.teste@gmail.com",
		},
		&pb.User{
			Id:    "4",
			Name:  "Caique 4",
			Email: "caique4.teste@gmail.com",
		},
		&pb.User{
			Id:    "5",
			Name:  "Caique 5",
			Email: "caique5.teste@gmail.com",
		},
	}

	wait := make(chan int)

	go func() {
		for _, req := range reqs {
			fmt.Println("Sending user: ", req.Name)
			stream.Send(req)
			time.Sleep(time.Second * 2)
		}
		stream.CloseSend()
	}()

	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error receivind data: %v", err)
				break
			}
			fmt.Printf("Recebendo user %v com status %v\n", res.GetUser().GetName(), res.GetStatus())
		}
		close(wait)
	}()

	<-wait
}

using System;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MiniMQTestClient
{
    class Program
    {
        static CancellationTokenSource tokenSource = new CancellationTokenSource();

        static async Task<int> Main(string[] args)
        {
            if(args.Length == 0)
            {
                Console.WriteLine("Invalid arguments.");
                return 1;
            }

            if (args[0] == "p")
            {
                Producer(tokenSource.Token);
            }
            else if(args[0] == "c")
            {
                await Consumer(tokenSource.Token);
            }

            return 0;
        }

        static void Producer(CancellationToken cancellationToken)
        {
            // Data buffer for incoming data.
            byte[] bytes = new byte[4096];

            // Connect to a remote device.
            try
            {
                // Establish the remote endpoint for the socket.
                IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
                IPAddress ipAddress = ipHostInfo.AddressList[0];
                IPEndPoint remoteEP = new IPEndPoint(ipAddress, 11000);

                // Create a TCP/IP  socket.
                Socket sender = new Socket(ipAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                sender.Connect(remoteEP);

                Console.WriteLine("Socket connected to {0}", sender.RemoteEndPoint.ToString());

                int counter = 0;

                while (!cancellationToken.IsCancellationRequested)
                {
                    //Console.Write("Press enter to send.");
                    //Console.ReadLine();

                    // Encode the data string into a byte array.
                    byte[] body = Encoding.ASCII.GetBytes("This is a test");
                    var message = new byte[body.Length + 3];
                    message[0] = 0; // Put action.

                    ushort bodyLen = (ushort)body.Length;
                    short bodyLenNetwork = IPAddress.HostToNetworkOrder((short)bodyLen);
                    var bodyLenBytes = BitConverter.GetBytes(bodyLenNetwork);
                    message[1] = bodyLenBytes[0];
                    message[2] = bodyLenBytes[1];

                    Array.Copy(body, 0, message, 3, body.Length);

                    Console.WriteLine("Sending Message ({0})", counter);
                    int bytesSent = sender.Send(message);

                    int bytesRec = sender.Receive(bytes);
                    if (bytesRec > 0)
                    {
                        if (bytes[0] == 0)
                        {
                            if (bytesRec == 1)
                            {
                                Console.WriteLine("Received ACK.");
                            }
                            else
                            {
                                Console.WriteLine("Received ACK, but size unexpected: {0}", bytesRec);
                            }
                        }
                        else if (bytes[0] == 1)
                        {
                            if (bytesRec == 2)
                            {
                                Console.WriteLine("Received ERROR! Code = {0}", bytes[1]);
                            }
                            else if (bytesRec > 2)
                            {
                                Console.WriteLine("Received ERROR! Code = {0}, but unexpected size: {1}", bytes[1], bytesRec);
                            }
                            else if (bytesRec < 2)
                            {
                                Console.WriteLine("Received ERROR, but no error code!");
                            }
                        }
                    }

                    counter++;

                    // Thread.Sleep(1000);
                    // Thread.Sleep(100);
                }

                sender.Shutdown(SocketShutdown.Both);
                sender.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: {0}", ex);
            }
        }

        static async Task Consumer(CancellationToken cancellationToken)
        {
            // Data buffer for incoming data.
            byte[] bytes = new byte[4096];

            int counter = 0;

            try
            {
                IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
                IPAddress ipAddress = ipHostInfo.AddressList[0];
                IPEndPoint remoteEP = new IPEndPoint(ipAddress, 11001);

                // Create a TCP/IP  socket.
                using var sender = new Socket(ipAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                sender.Connect(remoteEP);

                while(!cancellationToken.IsCancellationRequested)
                {
                    int bytesRec = await sender.ReceiveAsync(bytes, SocketFlags.None, cancellationToken);

                    // TODO: Decode message.
                    Console.WriteLine("Got {0} bytes from queue ({1}).", bytesRec, counter);

                    // Send ACK to acknowledge the message was accepted.
                    var ack = new byte[1];
                    ack[0] = 0;
                    await sender.SendAsync(ack, SocketFlags.None, cancellationToken);

                    Console.WriteLine("ACK sent.");

                    counter++;
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("Exception: {0}", ex);
            }
        }
    }
}

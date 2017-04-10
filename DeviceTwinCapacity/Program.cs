using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Common.Exceptions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using DotNetty.Common.Utilities;
using Microsoft.Azure.Devices.Shared;
using TransportType = Microsoft.Azure.Devices.Client.TransportType;

namespace DeviceTwinCapacity
{
    class Program
    {
        private static readonly string IotHubConnectionString = "{Your IoT Hub Connection String}";
        private static RegistryManager _registryManager;
        private const string TestDevicePrefix = "TestCapacityDevice";
        private const int MaxDeviceNum = 500;
        static void Main(string[] args)
        {
            _registryManager = RegistryManager.CreateFromConnectionString(IotHubConnectionString);
            Task.Run(async () =>
            {
                await CreateDevicesOnIoTHub();
                var deviceList = await GetDevices();
                await GetTwinsFromServerParallel(deviceList);
            });

            Console.ReadLine();
        }


        private static async Task CreateDevicesOnIoTHub()
        {
            Task[] createDeviceTasks = new Task[MaxDeviceNum];
            Console.WriteLine("Create 100 devices parallel...");

            for (var i = 0; i < MaxDeviceNum; i++)
            {
                await CreateDeviceIdentityAsync(TestDevicePrefix + i);
            }

            Console.WriteLine("Create 100 devices success!");
        }




        private static async Task GetTwinsFromServerParallel(List<DeviceEntity> deviceList)
        {
            Console.WriteLine("Get all device twins parallel...");
            Stopwatch stopwatch = Stopwatch.StartNew();


            List<Task> getTwinTasks = new List<Task>();
            var successNum = 0;
            for (var i = 0; i < deviceList.Count; i++)
            {
                getTwinTasks.Add(Task.Factory.StartNew(async (object obj) =>
                {
                    var deviceEntity = obj as DeviceEntity;
                    if (deviceEntity == null)
                        return;

                    var deviceClient = DeviceClient.CreateFromConnectionString(deviceEntity.ConnectionString, TransportType.Mqtt_Tcp_Only);
                    Twin twin;
                    try
                    {
                        deviceClient.RetryPolicy = RetryPolicyType.Exponential_Backoff_With_Jitter;
                        twin = await deviceClient.GetTwinAsync();
                        successNum++;
                        Console.WriteLine("Get Twin for device:" + deviceEntity.Id + " success!\n" + twin.ToJson());
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Get twin error : " + deviceEntity.Id + "\n" + e);
                    }

                }, deviceList[i]).Unwrap());
            }

            await Task.WhenAll(getTwinTasks);

            stopwatch.Stop();
            Console.WriteLine("Get all twin complete!\nsuccess num:"+successNum+"\nTime elapsed:" + stopwatch.ElapsedMilliseconds+"\n");
        }

        private static async Task CreateDeviceIdentityAsync(string deviceName)
        {
            Console.WriteLine("Try to create device:" + deviceName);
            try
            {
                await _registryManager.AddDeviceAsync(new Device(deviceName));
            }
            catch (DeviceAlreadyExistsException)
            {
                //await _registryManager.GetDeviceAsync(deviceName);
                Console.WriteLine("Already existed: " + deviceName);
                return;
            }
            Console.WriteLine("Create success: " + deviceName);
        }

        private static async Task<List<DeviceEntity>> GetDevices()
        {

            var devices = await _registryManager.GetDevicesAsync(500);
            Console.WriteLine("Get All devices infomation...");
            var listOfDevices = new List<DeviceEntity>();
            if (devices != null)
            {
                foreach (var device in devices)
                {
                    var deviceEntity = new DeviceEntity()
                    {
                        Id = device.Id,
                        ConnectionState = device.ConnectionState.ToString(),
                        ConnectionString = CreateDeviceConnectionString(device),
                        LastActivityTime = device.LastActivityTime,
                        LastConnectionStateUpdatedTime = device.ConnectionStateUpdatedTime,
                        LastStateUpdatedTime = device.StatusUpdatedTime,
                        MessageCount = device.CloudToDeviceMessageCount,
                        State = device.Status.ToString(),
                        SuspensionReason = device.StatusReason,
                    };

                    if (device.Authentication != null)
                    {

                        deviceEntity.PrimaryKey = device.Authentication.SymmetricKey?.PrimaryKey;
                        deviceEntity.SecondaryKey = device.Authentication.SymmetricKey?.SecondaryKey;
                        deviceEntity.PrimaryThumbPrint = device.Authentication.X509Thumbprint?.PrimaryThumbprint;
                        deviceEntity.SecondaryThumbPrint = device.Authentication.X509Thumbprint?.SecondaryThumbprint;
                    }

                    listOfDevices.Add(deviceEntity);
                }
            }
            Console.WriteLine("Get All devices infomation success!");
            return listOfDevices;
        }

        private static String CreateDeviceConnectionString(Device device)
        {
            var deviceConnectionString = new StringBuilder();

            var hostName = String.Empty;
            var tokenArray = IotHubConnectionString.Split(';');
            foreach (var t in tokenArray)
            {
                var keyValueArray = t.Split('=');
                if (keyValueArray[0] == "HostName")
                {
                    hostName = t + ';';
                    break;
                }
            }

            if (!string.IsNullOrWhiteSpace(hostName))
            {
                deviceConnectionString.Append(hostName);
                deviceConnectionString.AppendFormat("DeviceId={0}", device.Id);

                if (device.Authentication != null)
                {
                    if (device.Authentication.SymmetricKey?.PrimaryKey != null)
                    {
                        deviceConnectionString.AppendFormat(";SharedAccessKey={0}", device.Authentication.SymmetricKey.PrimaryKey);
                    }
                    else
                    {
                        deviceConnectionString.AppendFormat(";x509=true");
                    }
                }
            }

            return deviceConnectionString.ToString();
        }
    }
}

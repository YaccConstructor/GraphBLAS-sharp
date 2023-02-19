namespace GraphBLAS.FSharp.Backend.Objects

open Brahma.FSharp

module ClContext =
    type AllocationFlag =
        | DeviceOnly
        | HostInterop

    type ClContext with
        member this.CreateClArrayWithSpecificAllocationMode(mode, size: int) =
            match mode with
            | DeviceOnly ->
                this.CreateClArray(
                    size,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )
            | HostInterop ->
                this.CreateClArray(
                    size,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

        member this.CreateClArrayWithSpecificAllocationMode(mode, array: 'a []) =
            match mode with
            | DeviceOnly ->
                this.CreateClArray(
                    array,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.CopyHostPtr
                )
            | HostInterop ->
                this.CreateClArray(
                    array,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.ReadWrite,
                    allocationMode = AllocationMode.CopyHostPtr
                )

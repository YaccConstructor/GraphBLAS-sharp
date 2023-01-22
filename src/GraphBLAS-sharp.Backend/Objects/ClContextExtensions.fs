namespace GraphBLAS.FSharp.Backend.Objects

open Brahma.FSharp

module ClContext =
    type AllocationFlag =
        | GPUOnly
        | CPUInterop

    type ClContext with
        member this.CreateClArrayWithFlag(mode, (size: int)) =
            match mode with
            | GPUOnly ->
                this.CreateClArray(
                    size,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )
            | CPUInterop ->
                this.CreateClArray(
                    size,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

        member this.CreateClArrayWithFlag(mode, (array: 'a [])) =
            match mode with
            | GPUOnly ->
                this.CreateClArray(
                    array,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.CopyHostPtr
                )
            | CPUInterop ->
                this.CreateClArray(
                    array,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.ReadWrite,
                    allocationMode = AllocationMode.CopyHostPtr
                )

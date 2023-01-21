namespace GraphBLAS.FSharp.Backend.Objects

open Brahma.FSharp

module ClContext =
    type ClContext with
        member this.CreateClArrayWithGPUOnlyFlags(size: int) =
            this.CreateClArray(
                size,
                deviceAccessMode = DeviceAccessMode.ReadWrite,
                hostAccessMode = HostAccessMode.NotAccessible,
                allocationMode = AllocationMode.Default
            )

        member this.CreateClArrayWithGPUOnlyFlags(array: 'a []) =
            this.CreateClArray(
                array,
                deviceAccessMode = DeviceAccessMode.ReadWrite,
                hostAccessMode = HostAccessMode.NotAccessible,
                allocationMode = AllocationMode.CopyHostPtr
            )

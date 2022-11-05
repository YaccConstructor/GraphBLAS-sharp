namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.ArraysExtensions
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module Vector =
    let spMV
        (clContext: ClContext)
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =
        //Until LocalMemSize added to ClDevice as member
        let error = ref Unchecked.defaultof<ClErrorCode>

        let localMemorySize =
            Cl
                .GetDeviceInfo(clContext.ClDevice.Device, OpenCL.Net.DeviceInfo.LocalMemSize, error)
                .CastTo<int>()

        let localArraySize1 = workGroupSize + 1

        let localMemoryLeft =
            localMemorySize - localArraySize1 * sizeof<int>

        let optionTypeClSizeInBytes =
            4 + sizeof<'c>
            |> Utils.ceilToMultiple (max sizeof<'c> sizeof<int>)

        let localArraySize2 =
            localMemoryLeft / optionTypeClSizeInBytes
            |> Utils.floorToMultiple workGroupSize

        let kernel1 =
            <@ fun (ndRange: Range1D) matrixLength (matrixColumns: ClArray<int>) (matrixValues: ClArray<'a>) (vectorValues: ClArray<'b option>) (intermediateArray: ClArray<'c option>) ->

                let i = ndRange.GlobalID0
                let value = matrixValues.[i]
                let column = matrixColumns.[i]

                if i < matrixLength then
                    intermediateArray.[i] <- (%mul) (Some value) vectorValues.[column] @>

        let kernel2 =
            <@ fun (ndRange: Range1D) (numberOfRows: int) (intermediateArray: ClArray<'c option>) (matrixPtr: ClArray<int>) (outputVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                if gid <= numberOfRows then
                    let threadsPerBlock =
                        min (numberOfRows - gid + lid) workGroupSize //If number of rows left is lesser than number of threads in a block

                    let localPtr = localArray<int> localArraySize1
                    localPtr.[lid] <- matrixPtr.[gid]

                    if lid = 0 then
                        localPtr.[threadsPerBlock] <- matrixPtr.[gid + threadsPerBlock]

                    barrierLocal ()

                    let localValues = localArray<'c option> localArraySize2
                    let workEnd = localPtr.[threadsPerBlock]
                    let mutable blockLowerBound = localPtr.[0]
                    let numberOfBlocksFitting = localArraySize2 / threadsPerBlock
                    let workPerIteration = threadsPerBlock * numberOfBlocksFitting

                    let mutable sum: 'c option = None

                    while blockLowerBound < workEnd do
                        let mutable index = blockLowerBound + lid

                        barrierLocal ()
                        //Loading values to the local memory
                        for block in 0 .. numberOfBlocksFitting - 1 do
                            if index < workEnd then
                                localValues.[lid + block * threadsPerBlock] <- intermediateArray.[index]
                                index <- index + threadsPerBlock

                        barrierLocal ()
                        //Reduction
                        //Check if any part of the row is loaded into local memory on this iteration
                        if (localPtr.[lid + 1] > blockLowerBound
                            && localPtr.[lid] < blockLowerBound + workPerIteration) then
                            let rowStart = max (localPtr.[lid] - blockLowerBound) 0

                            let rowEnd =
                                min (localPtr.[lid + 1] - blockLowerBound) workPerIteration

                            for jj in rowStart .. rowEnd - 1 do
                                match (%add) sum localValues.[jj] with
                                | Some v -> sum <- Some v
                                | None -> sum <- None

                        blockLowerBound <- blockLowerBound + workPerIteration

                    if gid < numberOfRows then
                        outputVector.[gid] <- sum @>

        let kernel1 = clContext.Compile kernel1
        let kernel2 = clContext.Compile kernel2

        fun (queue: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) (vector: ClArray<'b option>) ->

            let matrixLength = matrix.Values.Length

            let ndRange1 =
                Range1D.CreateValid(matrixLength, workGroupSize)

            let ndRange2 =
                Range1D.CreateValid(matrix.RowCount, workGroupSize)

            let intermediateArray =
                clContext.CreateClArray<'c option>(
                    matrixLength,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let kernel1 = kernel1.GetKernel()

            queue.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel1.KernelFunc ndRange1 matrixLength matrix.Columns matrix.Values vector intermediateArray)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(kernel1))

            let outputArray =
                clContext.CreateClArray<'c option>(
                    matrix.RowCount,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    allocationMode = AllocationMode.Default
                )

            let kernel2 = kernel2.GetKernel()

            queue.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel2.KernelFunc ndRange2 matrix.RowCount intermediateArray matrix.RowPointers outputArray)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(kernel2))

            queue.Post(Msg.CreateFreeMsg intermediateArray)

            outputArray

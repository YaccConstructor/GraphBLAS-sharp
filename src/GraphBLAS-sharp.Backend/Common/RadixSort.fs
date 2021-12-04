namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Core.Operators
open GraphBLAS.FSharp.Backend

module internal rec RadixSort =
    let sortByKeyInPlace (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>)
            (keys: ClArray<uint64>)
            (values: ClArray<'a>)
            (numberOfBits: int) ->

            let numberOfArrays = pown 2 numberOfBits
            let numberOfStages = 64 / numberOfBits

            let allPositions =
                [| for i in 0 .. numberOfArrays - 1 ->
                    ClArray.create clContext workGroupSize processor keys.Length 0
                    // clContext.CreateClArray(
                    //     keys.Length,
                    //     hostAccessMode = HostAccessMode.NotAccessible
                    // )
                |]

            let sums =
                clContext.CreateClArray(
                    numberOfArrays,
                    hostAccessMode = HostAccessMode.NotAccessible
                )

            let auxiliaryKeys =
                if numberOfStages % 2 = 0 then
                    clContext.CreateClArray(
                        keys.Length,
                        hostAccessMode = HostAccessMode.NotAccessible
                    )
                else
                    ClArray.copy clContext processor workGroupSize keys

            let auxiliaryValues =
                if numberOfStages % 2 = 0 then
                    clContext.CreateClArray(
                        values.Length,
                        hostAccessMode = HostAccessMode.NotAccessible
                    )
                else
                    ClArray.copy clContext processor workGroupSize values

            let swap (a, b) = (b, a)

            let mutable keysArrays = auxiliaryKeys, keys
            let mutable valuesArrays = auxiliaryValues, values
            if numberOfStages % 2 = 0 then
                keysArrays <- swap keysArrays
                valuesArrays <- swap valuesArrays

            for stage in 0 .. numberOfStages - 1 do
                preparePositions
                    clContext
                    workGroupSize
                    processor
                    (fst keysArrays)
                    allPositions
                    sums
                    numberOfBits
                    stage

                setPositions
                    clContext
                    workGroupSize
                    processor
                    allPositions
                    (fst keysArrays)
                    (fst valuesArrays)
                    (snd keysArrays)
                    (snd valuesArrays)
                    numberOfBits
                    stage

                keysArrays <- swap keysArrays
                valuesArrays <- swap valuesArrays

            allPositions
            |> Array.iter (fun positions ->
                processor.Post(Msg.CreateFreeMsg(positions))
            )
            processor.Post(Msg.CreateFreeMsg(sums))
            processor.Post(Msg.CreateFreeMsg(auxiliaryKeys))
            processor.Post(Msg.CreateFreeMsg(auxiliaryValues))


    let private setPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (allPositions: ClArray<int>[])
        (keys: ClArray<uint64>)
        (values: ClArray<'a>)
        (resultKeys: ClArray<uint64>)
        (resultValues: ClArray<'a>)
        (numberOfBits: int)
        (stage: int) =

        let keysLength = keys.Length
        let offset = numberOfBits * stage

        let updatePositions =
            <@
                fun (ndRange: Range1D)
                    (keys: ClArray<uint64>)
                    (values: ClArray<'a>)
                    (positions: ClArray<int>)
                    (indexOfPositions: int)
                    (resultKeys: ClArray<uint64>)
                    (resultValues: ClArray<'a>) ->

                    let i = ndRange.GlobalID0

                    if i < keysLength then
                        let mutable mask = 0UL
                        for j in 0 .. numberOfBits - 1 do
                            mask <- (mask <<< 1) ||| 1UL
                        if (keys.[i] >>> offset) &&& mask = uint64 indexOfPositions then
                        // if (keys.[i] >>> offset) &&& ~~~(0xFFFFFFFFFFFFFFFFUL <<< numberOfBits) = uint64 indexOfPositions then
                            resultKeys.[positions.[i]] <- keys.[i]
                            resultValues.[positions.[i]] <- values.[i]
            @>

        let ndRange = Range1D.CreateValid(keysLength, workGroupSize)

        // TODO: сделать параллельно
        allPositions
        |> Array.iteri (fun i positions ->
            let kernel = clContext.CreateClKernel updatePositions

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.ArgumentsSetter
                                ndRange
                                keys
                                values
                                positions
                                i
                                resultKeys
                                resultValues)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)
        )

    let private updatePositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (allPositions: ClArray<int>[])
        positionsLength
        (sums: ClArray<int>) =

        let updatePositions =
            <@
                fun (ndRange: Range1D)
                    (positions: ClArray<int>)
                    (indexOfPositions: int)
                    (sums: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    if i < positionsLength then
                        positions.[i] <- positions.[i] + sums.[indexOfPositions]
            @>

        let ndRange = Range1D.CreateValid(positionsLength, workGroupSize)

        // TODO: сделать параллельно
        allPositions
        |> Array.skip 1
        |> Array.iteri (fun i positions ->
            let kernel = clContext.CreateClKernel updatePositions

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.ArgumentsSetter ndRange positions (i + 1) sums)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)
        )

    let private preparePositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (keys: ClArray<uint64>)
        (allPositions: ClArray<int>[])
        (sums: ClArray<int>)
        (numberOfBits: int)
        (stage: int) =

        let keysLength = keys.Length
        let offset = numberOfBits * stage

        let preparePositions =
            <@
                fun (ndRange: Range1D)
                    (keys: ClArray<uint64>)
                    (positions: ClArray<int>)
                    (indexOfPositions: int) ->

                    let i = ndRange.GlobalID0

                    if i < keysLength then
                        let mutable mask = 0UL
                        for j in 0 .. numberOfBits - 1 do
                            mask <- (mask <<< 1) ||| 1UL
                        if (keys.[i] >>> offset) &&& mask = uint64 indexOfPositions then
                        // if (keys.[i] >>> offset) &&& ~~~(0xFFFFFFFFFFFFFFFFUL <<< numberOfBits) = uint64 indexOfPositions then
                            positions.[i] <- 1
                        else
                            positions.[i] <- 0
            @>


        let ndRange = Range1D.CreateValid(keysLength, workGroupSize)

        // TODO: сделать параллельно
        allPositions
        |> Array.iteri (fun i positions ->
            let kernel = clContext.CreateClKernel preparePositions

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.ArgumentsSetter ndRange keys positions i)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            PrefixSum.runExcludeInplace clContext workGroupSize processor positions sums i <@ (+) @> 0
            |> ignore
        )

        // TODO: не используется
        let total = clContext.CreateClArray(1)

        PrefixSum.runExcludeInplace clContext workGroupSize processor sums total 0 <@ (+) @> 0
        |> ignore

        processor.Post(Msg.CreateFreeMsg(total))

        updatePositions clContext workGroupSize processor allPositions keys.Length sums

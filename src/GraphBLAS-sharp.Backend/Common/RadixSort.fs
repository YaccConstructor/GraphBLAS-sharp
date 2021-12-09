namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Core.Operators
open GraphBLAS.FSharp.Backend

module internal RadixSort =
    let private setPositions
        (getBy: Expr<'b -> uint64 -> int>)
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (positions: ClArray<_>)
        (keys: ClArray<uint64>)
        (values: ClArray<'a>)
        (resultKeys: ClArray<uint64>)
        (resultValues: ClArray<'a>)
        (numberOfBits: int)
        (stage: int) =

        let keysLength = keys.Length
        let offset = numberOfBits * stage

        let setPositions =
            <@
                fun (ndRange: Range1D)
                    (keys: ClArray<uint64>)
                    (values: ClArray<'a>)
                    (positions: ClArray<_>)
                    (resultKeys: ClArray<uint64>)
                    (resultValues: ClArray<'a>) ->

                    let i = ndRange.GlobalID0

                    if i < keysLength then
                        let mutable mask = 0UL
                        for j in 0 .. numberOfBits - 1 do
                            mask <- (mask <<< 1) ||| 1UL
                        let buff = positions.[i]
                        let index = (%getBy) buff ((keys.[i] >>> offset) &&& mask)
                        //let index = (%get) buff ((keys.[i] >>> offset) &&& ~~~(0xFFFFFFFFFFFFFFFFUL <<< numberOfBits))
                        resultKeys.[index] <- keys.[i]
                        resultValues.[index] <- values.[i]
            @>

        let ndRange = Range1D.CreateValid(keysLength, workGroupSize)

        let kernel = clContext.CreateClKernel setPositions

        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        keys
                        values
                        positions
                        resultKeys
                        resultValues)
        )

        processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private updatePositions
        (plus: Expr<'b -> 'b -> 'b>)
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (positions: ClArray<_>)
        positionsLength
        (sums: ClCell<_>) =

        let updatePositions =
            <@
                fun (ndRange: Range1D)
                    (positions: ClArray<_>)
                    (sums: ClCell<_>) ->

                    let i = ndRange.GlobalID0

                    if i < positionsLength then
                        let sums = sums.Value
                        let buff = positions.[i]
                        positions.[i] <- (%plus) buff sums
            @>

        let ndRange = Range1D.CreateValid(positionsLength, workGroupSize)

        let kernel = clContext.CreateClKernel updatePositions

        processor.Post(
            Msg.MsgSetArguments
                (fun () -> kernel.ArgumentsSetter ndRange positions sums)
        )

        processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private updateSums
        (scan: Expr<'b -> 'b>)
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (sums: ClCell<_>) =

        let update =
            <@
                fun (ndRange: Range1D)
                    (sums: ClCell<'b>) ->

                    let i = ndRange.GlobalID0
                    if i = 0 then
                        let a = sums.Value
                        sums.Value <- (%scan) a
            @>

        let ndRange = Range1D(workGroupSize)

        let kernel = clContext.CreateClKernel update

        processor.Post(
            Msg.MsgSetArguments
                (fun () -> kernel.ArgumentsSetter ndRange sums)
        )

        processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private initPositions
        (get: Expr<uint64 -> 'b>)
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (keys: ClArray<uint64>)
        (positions: ClArray<_>)
        (numberOfBits: int)
        (stage: int) =

        let keysLength = keys.Length
        let offset = numberOfBits * stage

        let init =
            <@
                fun (ndRange: Range1D)
                    (keys: ClArray<uint64>)
                    (positions: ClArray<_>) ->

                    let i = ndRange.GlobalID0

                    if i < keysLength then
                        let mutable mask = 0UL
                        for j in 0 .. numberOfBits - 1 do
                            mask <- (mask <<< 1) ||| 1UL
                        positions.[i] <- (%get) ((keys.[i] >>> offset) &&& mask)

                        //let buff = keys.[i]
                        //positions.[i] <- (%get) ((buff >>> offset) &&& ~~~(0xFFFFFFFFFFFFFFFFUL <<< numberOfBits))
            @>

        let ndRange = Range1D.CreateValid(keysLength, workGroupSize)

        let kernel = clContext.CreateClKernel init

        processor.Post(
            Msg.MsgSetArguments
                (fun () -> kernel.ArgumentsSetter ndRange keys positions)
        )

        processor.Post(Msg.CreateRunMsg<_, _> kernel)

    let private preparePositions
        (plus: Expr<'b -> 'b -> 'b>)
        (get: Expr<uint64 -> 'b>)
        (scan: Expr<'b -> 'b>)
        (zero: 'b)
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (keys: ClArray<uint64>)
        (positions: ClArray<_>)
        (sums: ClCell<_>)
        (numberOfBits: int)
        (stage: int) =

        initPositions get clContext workGroupSize processor keys positions numberOfBits stage

        PrefixSum.runExcludeInplace clContext workGroupSize processor positions sums plus zero
        |> ignore

        updateSums scan clContext workGroupSize processor sums

        updatePositions plus clContext workGroupSize processor positions keys.Length sums

    // TODO: сделать через ClArray.create
    let private createPositions
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (length: int)
        (zero: 'b) =

        let zero = clContext.CreateClArray([|zero|])

        let positions = clContext.CreateClArray(length)

        let init =
            <@
                fun (range: Range1D)
                    (zero: ClArray<'b>)
                    (positions: ClArray<'b>) ->

                    let i = range.GlobalID0
                    let zero = zero.[0]
                    if i < length then
                        positions.[i] <- zero
            @>

        let kernel = clContext.CreateClKernel init
        let ndRange = Range1D.CreateValid(length, workGroupSize)
        processor.Post(
            Msg.MsgSetArguments(fun () -> kernel.ArgumentsSetter ndRange zero positions)
        )
        processor.Post(Msg.CreateRunMsg<_, _> kernel)

        positions

    let private sortByKeyInPlaceGeneral
        (plus: Expr<'b -> 'b -> 'b>)
        (get: Expr<uint64 -> 'b>)
        (getBy: Expr<'b -> uint64 -> int>)
        (scan: Expr<'b -> 'b>)
        (zero: 'b)
        (numberOfBits: int)
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (keys: ClArray<uint64>)
        (values: ClArray<'a>) =

        let numberOfStages = (64 - 1) / numberOfBits + 1

        let positions = createPositions clContext workGroupSize processor keys.Length zero
        let sums = clContext.CreateClCell()

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
                plus
                get
                scan
                zero
                clContext
                workGroupSize
                processor
                (fst keysArrays)
                positions
                sums
                numberOfBits
                stage

            setPositions
                getBy
                clContext
                workGroupSize
                processor
                positions
                (fst keysArrays)
                (fst valuesArrays)
                (snd keysArrays)
                (snd valuesArrays)
                numberOfBits
                stage

            keysArrays <- swap keysArrays
            valuesArrays <- swap valuesArrays

        processor.Post(Msg.CreateFreeMsg(positions))
        processor.Post(Msg.CreateFreeMsg(sums))
        processor.Post(Msg.CreateFreeMsg(auxiliaryKeys))
        processor.Post(Msg.CreateFreeMsg(auxiliaryValues))

    let sortByKeyInPlace2 clContext =
        sortByKeyInPlaceGeneral
            <@
                fun (struct(x1, x2): struct(int * int))
                    (struct(y1, y2): struct(int * int)) ->
                    struct(x1 + y1, x2 + y2)
            @>
            <@
                fun (n: uint64) ->
                    if n = 0UL then struct(1, 0)
                    elif n = 1UL then struct(0, 1)
                    else struct(0, 0)
            @>
            <@
                fun (struct(x1, x2): struct(int * int))
                    (n: uint64) ->
                    if n = 0UL then x1
                    elif n = 1UL then x2
                    else 0
            @>
            <@
                fun (struct(x1, x2): struct(int * int)) ->
                    struct(0, x1)
            @>
            struct(0, 0)
            1
            clContext

    let sortByKeyInPlace4 clContext =
        sortByKeyInPlaceGeneral
            <@
                fun (struct(x1, x2, x3, x4): struct(int * int * int * int))
                    (struct(y1, y2, y3, y4): struct(int * int * int * int)) ->
                    struct(x1 + y1, x2 + y2, x3 + y3, x4 + y4)
            @>
            <@
                fun (n: uint64) ->
                    if n = 0UL then struct(1, 0, 0, 0)
                    elif n = 1UL then struct(0, 1, 0, 0)
                    elif n = 2UL then struct(0, 0, 1, 0)
                    elif n = 3UL then struct(0, 0, 0, 1)
                    else struct(0, 0, 0, 0)
            @>
            <@
                fun (struct(x1, x2, x3, x4): struct(int * int * int * int))
                    (n: uint64) ->
                    if n = 0UL then x1
                    elif n = 1UL then x2
                    elif n = 2UL then x3
                    elif n = 3UL then x4
                    else 0
            @>
            <@
                fun (struct(x1, x2, x3, x4): struct(int * int * int * int)) ->
                    struct(0, x1, x1 + x2, x1 + x2 + x3)
            @>
            struct(0, 0, 0, 0)
            2
            clContext

    let sortByKeyInPlace8 clContext =
        sortByKeyInPlaceGeneral
            <@
                fun (struct(x1, x2, x3, x4, x5, x6, x7, x8): struct(int * int * int * int * int * int * int * int))
                    (struct(y1, y2, y3, y4, y5, y6, y7, y8): struct(int * int * int * int * int * int * int * int)) ->
                    struct(x1 + y1, x2 + y2, x3 + y3, x4 + y4, x5 + y5, x6 + y6, x7 + y7, x8 + y8)
            @>
            <@
                fun (n: uint64) ->
                    if n = 0UL then struct(1, 0, 0, 0, 0, 0, 0, 0)
                    elif n = 1UL then struct(0, 1, 0, 0, 0, 0, 0, 0)
                    elif n = 2UL then struct(0, 0, 1, 0, 0, 0, 0, 0)
                    elif n = 3UL then struct(0, 0, 0, 1, 0, 0, 0, 0)
                    elif n = 4UL then struct(0, 0, 0, 0, 1, 0, 0, 0)
                    elif n = 5UL then struct(0, 0, 0, 0, 0, 1, 0, 0)
                    elif n = 6UL then struct(0, 0, 0, 0, 0, 0, 1, 0)
                    elif n = 7UL then struct(0, 0, 0, 0, 0, 0, 0, 1)
                    else struct(0, 0, 0, 0, 0, 0, 0, 0)
            @>
            <@
                fun (struct(x1, x2, x3, x4, x5, x6, x7, x8): struct(int * int * int * int * int * int * int * int))
                    (n: uint64) ->
                    if n = 0UL then x1
                    elif n = 1UL then x2
                    elif n = 2UL then x3
                    elif n = 3UL then x4
                    elif n = 4UL then x5
                    elif n = 5UL then x6
                    elif n = 6UL then x7
                    elif n = 7UL then x8
                    else 0
            @>
            <@
                fun (struct(x1, x2, x3, x4, x5, x6, x7, x8): struct(int * int * int * int * int * int * int * int)) ->
                    let y2 = x1 + x2
                    let y3 = y2 + x3
                    let y4 = y3 + x4
                    let y5 = y4 + x5
                    let y6 = y5 + x6
                    let y7 = y6 + x7
                    struct(0, x1, y2, y3, y4, y5, y6, y7)
            @>
            struct(0, 0, 0, 0, 0, 0, 0, 0)
            3
            clContext

namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp

module internal Gather =
    let runInit positionMap (clContext: ClContext) workGroupSize =

        let gather =
            <@ fun (ndRange: Range1D) valuesLength (values: ClArray<'a>) (outputArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0

                if gid < valuesLength then
                    let position = (%positionMap) gid

                    if position >= 0 && position < valuesLength then
                        outputArray.[gid] <- values.[position] @>

        let program = clContext.Compile gather

        fun (processor: MailboxProcessor<_>) (values: ClArray<'a>) (outputArray: ClArray<'a>) ->

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(outputArray.Length, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange values.Length values outputArray))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))


    /// <summary>
    /// Creates a new array obtained from positions replaced with values from the given array at these positions (indices).
    /// </summary>
    /// <example>
    /// <code>
    /// let positions = [| 2; 0; 2; 1 |]
    /// let array = [| 1.4; 2.5; 3.6 |]
    /// ...
    /// > val result = [| 3.6; 1.4; 3.6; 2.5 |]
    /// </code>
    /// </example>
    let run (clContext: ClContext) workGroupSize =

        let gather =
            <@ fun (ndRange: Range1D) positionsLength valuesLength (positions: ClArray<int>) (values: ClArray<'a>) (outputArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0

                if gid < positionsLength then
                    let position = positions.[gid]

                    if position >= 0 && position < valuesLength then
                        outputArray.[gid] <- values.[position] @>

        let program = clContext.Compile gather

        fun (processor: MailboxProcessor<_>) (positions: ClArray<int>) (values: ClArray<'a>) (outputArray: ClArray<'a>) ->

            if positions.Length <> outputArray.Length then
                failwith "Lengths must be the same"

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(positions.Length, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange positions.Length values.Length positions values outputArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

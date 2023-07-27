namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp

module Gather =
    /// <summary>
    /// Fills the given output array using the given value array and a function. The function maps old position
    /// of each element of the value array to its position in the output array.
    /// </summary>
    /// <remarks>
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positions = [| 1; 0; 2; 6; 4; 3; 5 |]
    /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; |]
    /// run clContext 32 processor positions values result
    /// ...
    /// > val result = [| 2.8; 1.9; 3.7; 7.3; 5.5; 4.6; 6.4 |]
    /// </code>
    /// </example>
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
    /// Fills the given output array using the given value and position arrays. Array of positions indicates
    /// which element from the value array should be in each position of the output array.
    /// </summary>
    /// <remarks>
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positions = [| 1; 0; 2; 6; 4; 3; 5 |]
    /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; |]
    /// run clContext 32 processor positions values result
    /// ...
    /// > val result = [| 2.8; 1.9; 3.7; 7.3; 5.5; 4.6; 6.4 |]
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

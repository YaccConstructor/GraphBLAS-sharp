namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp

module internal Scatter =
    let private general<'a> predicate (clContext: ClContext) workGroupSize =

        let run =
            <@ fun (ndRange: Range1D) (positions: ClArray<int>) (positionsLength: int) (values: ClArray<'a>) (result: ClArray<'a>) (resultLength: int) ->

                let gid = ndRange.GlobalID0

                if gid < positionsLength then
                    // positions lengths == values length
                    let predicateResult = (%predicate) gid positionsLength positions resultLength

                    if predicateResult then
                        result.[positions.[gid]] <- values.[gid] @>

        let program = clContext.Compile(run)

        fun (processor: MailboxProcessor<_>) (positions: ClArray<int>) (values: ClArray<'a>) (result: ClArray<'a>) ->

            if positions.Length <> values.Length then failwith "Lengths must be the same"

            let positionsLength = positions.Length

            let ndRange =
                Range1D.CreateValid(positionsLength, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange positions positionsLength values result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    /// <summary>
    /// Creates a new array from the given one where it is indicated by the array of positions at which position in the new array
    /// should be a value from the given one.
    /// </summary>
    /// <remarks>
    /// Every element of the positions array must not be less than the previous one.
    /// If there are several elements with the same indices, the FIRST one of them will be at the common index.
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
    /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
    /// run clContext 32 processor positions values result
    /// ...
    /// > val result = [| 1,9; 3.7; 6.4; 7.3; 9.1 |]
    /// </code>
    /// </example>
    let scatterFirstOccurrence clContext =
        general
        <| <@ fun gid _ (positions: ClArray<int>) resultLength ->
                 let currentKey = positions.[gid]
                 // first occurrence condition
                 (gid = 0 || positions.[gid - 1] <> positions.[gid])
                    // result position in valid range
                    && (0 <= currentKey && currentKey < resultLength)  @>
        <| clContext

    /// <summary>
    /// Creates a new array from the given one where it is indicated by the array of positions at which position in the new array
    /// should be a value from the given one.
    /// </summary>
    /// <remarks>
    /// Every element of the positions array must not be less than the previous one.
    /// If there are several elements with the same indices, the last one of them will be at the common index.
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
    /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
    /// run clContext 32 processor positions values result
    /// ...
    /// > val result = [| 2.8; 5.5; 6.4; 8.2; 9.1 |]
    /// </code>
    /// </example>
    let scatterLastOccurrence clContext =
        general
        <| <@ fun gid positionsLength (positions: ClArray<int>) resultLength ->
                let index = positions.[gid]
                // last occurrence condition
                (gid = positionsLength - 1 || index <> positions.[gid + 1])
                    // result position in valid range
                    && (0 <= index && index < resultLength) @>
        <| clContext

    /// <summary>
    /// Writes elements from the array of values to the array at the positions indicated by the global id map.
    /// </summary>
    /// <remarks>
    /// If index is out of bounds, the value will be ignored.
    /// </remarks>
    /// <example>
    /// <code>
    /// let positionMap = fun x -> x + 1
    /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
    /// let result = ... // create result
    /// run positionMap clContext 32 processor positions values result
    /// ...
    /// > val result = [| 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
    /// </code>
    /// </example>
    /// <param name="positionMap">Should be injective in order to avoid race conditions.</param>
    let init<'a> positionMap (clContext: ClContext) workGroupSize =

        let run =
            <@ fun (ndRange: Range1D) (valuesLength: int) (values: ClArray<'a>) (result: ClArray<'a>) resultLength ->

                let gid = ndRange.GlobalID0

                if gid < valuesLength then
                    let position = (%positionMap) gid

                    // may be race condition
                    if 0 <= position && position < resultLength then
                        result.[position] <- values.[gid] @>

        let program = clContext.Compile(run)

        fun (processor: MailboxProcessor<_>) (values: ClArray<'a>) (result: ClArray<'a>) ->

            let ndRange =
                Range1D.CreateValid(values.Length, workGroupSize)

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange values.Length values result result.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))


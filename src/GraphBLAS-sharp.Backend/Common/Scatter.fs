namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL

module internal Scatter =
    /// <summary>
    /// Creates a new array from the given one where it is indicated by array of positions at which position in the new array
    /// should be a value from the given one.
    /// </summary>
    /// <remarks>
    /// If there are several elements with same indices, the last one of them will be at the common index.
    /// </remarks>
    /// <example>
    /// <code>
    /// let ps = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
    /// let arr = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
    /// let res = run clContext 64 processor ps arr
    /// ...
    /// > val res = [| 2.8; 5.5; 6.4; 8.2; 9.1 |]
    /// </code>
    /// </example>
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">.</param>
    ///<param name="processor">.</param>
    ///<param name="inputArray">Values to scatter.</param>
    ///<param name="positions">
    /// Indices of the elements in the output array.
    /// The very first index must be zero.
    /// Every index must be the same as the previous one or more by one.
    /// </param>
    ///<param name="length">Length of the result array.</param>
    ///<param name="inputArray">Values to scatter.</param>
    let run
        (clContext: ClContext)
        workGroupSize
        (processor: MailboxProcessor<_>)
        (positions: ClArray<int>)
        (length: int)
        (inputArray: ClArray<'a>) =

        let positionsLength = positions.Length

        let scatter =
            <@
                fun (ndRange: Range1D)
                    (positions: ClArray<int>)
                    (inputArray: ClArray<'a>)
                    (outputArray: ClArray<'a>)
                    (length: int) ->

                    let i = ndRange.GlobalID0

                    if i < positionsLength then
                        let mutable buff = length
                        if i < positionsLength - 1 then
                            buff <- positions.[i + 1]
                        let index = positions.[i]
                        if index <> buff then
                            outputArray.[index] <- inputArray.[i]
            @>

        let outputArray =
            clContext.CreateClArray(
                length,
                hostAccessMode = HostAccessMode.NotAccessible,
                deviceAccessMode = DeviceAccessMode.WriteOnly
            )

        let kernel = clContext.CreateClKernel(scatter)

        let ndRange = Range1D.CreateValid(positionsLength, workGroupSize)

        processor.Post(
            Msg.MsgSetArguments
                (fun () ->
                    kernel.ArgumentsSetter
                        ndRange
                        positions
                        inputArray
                        outputArray
                        length)
        )

        processor.Post(Msg.CreateRunMsg<_, _>(kernel))

        outputArray

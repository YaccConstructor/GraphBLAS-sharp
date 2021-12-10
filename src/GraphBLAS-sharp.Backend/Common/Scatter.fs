namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL

module internal Scatter =
    /// <summary>
    /// Creates a new array from the given one where it is indicated by array of positions at which position in the new array
    /// should be a value from the given one.
    /// </summary>
    /// <remarks>
    /// If there are several elements with same indices, the last one of them will be at the common index.
    /// The very first element of the positions array must be zero.
    /// Every element of the positions array must not be less than the previous one.
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
    let run
        (clContext: ClContext)
        workGroupSize =

        let scatter =
            <@
                fun (ndRange: Range1D)
                    (positions: ClArray<int>)
                    (positionsLength: int)
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
        let kernel = clContext.CreateClKernel(scatter)

        fun (processor: MailboxProcessor<_>)
            (positions: ClArray<int>)
            (length: int)
            (inputArray: ClArray<'a>) ->

            let positionsLength = positions.Length

            let outputArray =
                clContext.CreateClArray(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly
                )
            let ndRange = Range1D.CreateValid(positionsLength, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            positions
                            positionsLength
                            inputArray
                            outputArray
                            length)
            )
            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            outputArray

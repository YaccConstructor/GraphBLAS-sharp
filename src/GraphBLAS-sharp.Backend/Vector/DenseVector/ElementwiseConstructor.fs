namespace GraphBLAS.FSharp.Backend.DenseVector

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common

module ElementwiseConstructor =
    let private elementWiseGeneralKernel writeOp =
        <@ fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0

                if gid < resultLength then
                     (%writeOp) gid leftVector rightVector resultVector @>

    let private elementWiseWrite opAdd =
        <@
           fun gid (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultArray: ClArray<'c option>) ->
                resultArray.[gid] <- (%opAdd) leftVector.[gid] rightVector.[gid]
        @>

    let private elementWiseAtLeastOneWrite opAdd =
        <@
           fun gid (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultArray: ClArray<'c option>) ->
            match leftVector.[gid], rightVector.[gid] with
            | Some left, Some right -> resultArray.[gid] <- (%opAdd) (Both(left, right))
            | Some left, None -> resultArray.[gid] <- (%opAdd) (Left left)
            | None, Some right -> resultArray.[gid] <- (%opAdd) (Right right)
            | _ -> resultArray.[gid] <- None
        @>

    let kernel opAdd = elementWiseGeneralKernel <| elementWiseWrite opAdd

    let atLeastOneKernel opAdd = elementWiseGeneralKernel <| elementWiseAtLeastOneWrite opAdd

    let private fillSubVectorGeneralKernel writeOp =
        <@
           fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (value: ClCell<'a>) (resultVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0

                if gid < resultLength then
                     (%writeOp) gid leftVector rightVector value.Value resultVector @>

    let private fillSubVectorWrite opAdd =
        <@
           fun gid (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (value: 'a) (resultArray: ClArray<'c option>) ->
                resultArray.[gid] <- (%opAdd) leftVector.[gid] rightVector.[gid] value
        @>

    let private fillSubVectorAtLeastOneWrite opAdd =
        <@
           fun gid (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (values: 'a) (resultArray: ClArray<'c option>) ->
            match leftVector.[gid], rightVector.[gid] with
            | Some left, Some right -> resultArray.[gid] <- (%opAdd) (Both(left, right)) values
            | Some left, None -> resultArray.[gid] <- (%opAdd) (Left left) values
            | None, Some right -> resultArray.[gid] <- (%opAdd) (Right right) values
            | _ -> resultArray.[gid] <- None
        @>

    let fillSubVector maskOp = fillSubVectorGeneralKernel <| fillSubVectorWrite maskOp

    let fillSubVectorAtLeastOne maskOp = fillSubVectorGeneralKernel <| fillSubVectorAtLeastOneWrite maskOp

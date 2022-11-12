namespace GraphBLAS.FSharp.Backend.DenseVector

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common

module ElementwiseConstructor =
    // let private elementWiseGeneralKernel writeOp =
    //     <@ fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->
    //
    //             let gid = ndRange.GlobalID0
    //
    //             if gid < resultLength then
    //                  resultVector[gid] <- (%writeOp) leftVector.[gid] rightVector.[gid] @>
    //
    // let private elementWiseWrite opAdd =
    //     <@
    //        fun (leftItem: 'a option) (rightItem: 'b option) ->
    //             (%opAdd) leftItem rightItem
    //     @>
    //
    // let private elementWiseAtLeastOneWrite opAdd =
    //     <@
    //        fun (leftItem: 'a option) (rightItem: 'b option) ->
    //         match leftItem, rightItem with
    //         | Some left, Some right -> (%opAdd) (Both(left, right))
    //         | Some left, None -> (%opAdd) (Left left)
    //         | None, Some right -> (%opAdd) (Right right)
    //         | _ -> None
    //     @>

    // let kernel opAdd = elementWiseGeneralKernel <| elementWiseWrite opAdd
    //
    // let atLeastOneKernel opAdd = elementWiseGeneralKernel <| elementWiseAtLeastOneWrite opAdd

    let kernel opAdd =
        <@
            fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                 let gid = ndRange.GlobalID0

                 if gid < resultLength then
                     resultVector[gid] <- (%opAdd) leftVector.[gid] rightVector.[gid]
        @>

    // let private fillSubVectorGeneralKernel writeOp =
    //     <@
    //        fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (value: ClCell<'a>) (resultVector: ClArray<'c option>) ->
    //
    //             let gid = ndRange.GlobalID0
    //
    //             if gid < resultLength then
    //                  resultVector[gid] <- (%writeOp) leftVector.[gid] rightVector.[gid] value.Value @>
    //
    // let private fillSubVectorWrite (opAdd: Expr<'a option -> 'b option -> 'a -> 'a option>) =
    //     <@
    //        fun (leftItem: 'a option) (rightItem: 'b option) (value: 'a) ->
    //             (%opAdd) leftItem rightItem value
    //     @>
    //
    // let private fillSubVectorAtLeastOneWrite (opAdd: Expr<AtLeastOne<'a, 'b>  -> 'a-> 'a option>) =
    //     <@
    //        fun (leftItem: 'a option) (rightItem: 'b option) (values: 'a) ->
    //         match leftItem, rightItem with
    //         | Some left, Some right -> (%opAdd) (Both(left, right)) values
    //         | Some left, None ->  (%opAdd) (Left left) values
    //         | None, Some right -> (%opAdd) (Right right) values
    //         | _ -> None
    //     @>

    // let fillSubVector maskOp = fillSubVectorGeneralKernel <| fillSubVectorWrite maskOp
    //
    // let fillSubVectorAtLeastOne maskOp = fillSubVectorGeneralKernel <| fillSubVectorAtLeastOneWrite maskOp
    let fillSubVectorKernel opAdd =
         <@
            fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (value: ClCell<'a>) (resultVector: ClArray<'c option>) ->

                 let gid = ndRange.GlobalID0

                 if gid < resultLength then
                     resultVector[gid] <- (%opAdd) leftVector.[gid] rightVector.[gid] value.Value @>

    let atLeastOneToNormalForm op =
        <@
            fun (leftItem: 'a option) (rightItem: 'b option) ->
                match leftItem, rightItem with
                | Some left, Some right ->
                    (%op) (Both(left, right))
                | None, Some right ->
                    (%op) (Right right)
                | Some left, None ->
                    (%op) (Left left)
                | None, None ->
                    None
        @>

    let fillSubVectorAtLeastOneToNormalForm op =
        <@
            fun (leftItem: 'a option) (rightItem: 'b option) (value: 'a) ->
                match leftItem, rightItem with
                | Some left, Some right ->
                    (%op) (Both(left, right)) value
                | None, Some right ->
                    (%op) (Right right) value
                | Some left, None ->
                    (%op) (Left left) value
                | None, None ->
                    None
        @>

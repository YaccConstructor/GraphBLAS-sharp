namespace GraphBLAS.FSharp.Backend.Quotes

open FSharp.Quotations

module Map =
    let optionToValueOrZero<'a> zero =
        <@ fun (item: 'a option) ->
            match item with
            | Some value -> value
            | None -> zero @>

    let option onSome onNone =
        <@ function
        | Some _ -> onSome
        | None -> onNone @>

    let id<'a> = <@ fun (item: 'a) -> item @>

    let chooseBitmap<'a, 'b> (map: Expr<'a -> 'b option>) =
        <@ fun (item: 'a) ->
            match (%map) item with
            | Some _ -> 1
            | None -> 0 @>

    let choose2Bitmap<'a, 'b, 'c> (map: Expr<'a -> 'b -> 'c option>) =
        <@ fun (leftItem: 'a) (rightItem: 'b) ->
            match (%map) leftItem rightItem with
            | Some _ -> 1
            | None -> 0 @>

    let predicateBitmap<'a> (predicate: Expr<'a -> bool>) =
        <@ fun (x: 'a) ->
            let res = (%predicate) x
            if res then 1 else 0 @>

    let inc = <@ fun item -> item + 1 @>

    let subtraction = <@ fun first second -> first - second @>

    let fst () = <@ fun fst _ -> fst @>

    let snd () = <@ fun _ snd -> snd @>

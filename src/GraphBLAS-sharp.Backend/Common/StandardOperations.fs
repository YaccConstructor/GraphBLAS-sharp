namespace GraphBLAS.FSharp.Backend.Common

open GraphBLAS.FSharp.Backend.Common

type AtLeastOne<'a, 'b when 'a: struct and 'b: struct> =
    | Both of 'a * 'b
    | Left of 'a
    | Right of 'b

module StandardOperations =
    let boolSum =
        <@ fun (_: bool option) (_: bool option) -> Some true @>

    let intSum =
        <@ fun (x: int option) (y: int option) ->
            let mutable res = 0

            match x, y with
            | Some f, Some s -> res <- f + s
            | Some f, None -> res <- f
            | None, Some s -> res <- s
            | None, None -> ()

            if res = 0 then None else (Some res) @>

    let byteSum =
        <@ fun (x: byte option) (y: byte option) ->
            let mutable res = 0uy

            match x, y with
            | Some f, Some s -> res <- f + s
            | Some f, None -> res <- f
            | None, Some s -> res <- s
            | None, None -> ()

            if res = 0uy then None else (Some res) @>

    let floatSum =
        <@ fun (x: float option) (y: float option) ->
            let mutable res = 0.0

            match x, y with
            | Some f, Some s -> res <- f + s
            | Some f, None -> res <- f
            | None, Some s -> res <- s
            | None, None -> ()

            if res = 0 then None else (Some res) @>

    let float32Sum =
        <@ fun (x: float32 option) (y: float32 option) ->
            let mutable res = 0.0f

            match x, y with
            | Some f, Some s -> res <- f + s
            | Some f, None -> res <- f
            | None, Some s -> res <- s
            | None, None -> ()

            if res = 0f then None else (Some res) @>

    let boolSumAtLeastOne =
        <@ fun (_: AtLeastOne<bool, bool>) -> Some true @>

    let intSumAtLeastOne =
        <@ fun (values: AtLeastOne<int, int>) ->
            let mutable res = 0

            match values with
            | Both (f, s) -> res <- f + s
            | Left f -> res <- f
            | Right s -> res <- s

            if res = 0 then None else (Some res) @>

    let byteSumAtLeastOne =
        <@ fun (values: AtLeastOne<byte, byte>) ->
            let mutable res = 0uy

            match values with
            | Both (f, s) -> res <- f + s
            | Left f -> res <- f
            | Right s -> res <- s

            if res = 0uy then None else (Some res) @>

    let floatSumAtLeastOne =
        <@ fun (values: AtLeastOne<float, float>) ->
            let mutable res = 0.0

            match values with
            | Both (f, s) -> res <- f + s
            | Left f -> res <- f
            | Right s -> res <- s

            if res = 0.0 then None else (Some res) @>

    let float32SumAtLeastOne =
        <@ fun (values: AtLeastOne<float32, float32>) ->
            let mutable res = 0f

            match values with
            | Both (f, s) -> res <- f + s
            | Left f -> res <- f
            | Right s -> res <- s

            if res = 0f then None else (Some res) @>

    let float32Mul =
        <@ fun (values: AtLeastOne<float32, float32>) ->
            let mutable res = 0f

            match values with
            | Both (f, s) -> res <- f * s
            | _ -> res <- 0f

            if res = 0f then None else (Some res) @>

    let boolMul =
        <@ fun (values: AtLeastOne<bool, bool>) ->
            let mutable res = false

            match values with
            | Both _ -> res <- true
            | _ -> res <- false

            if res then None else (Some true) @>

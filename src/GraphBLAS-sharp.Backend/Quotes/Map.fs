namespace GraphBLAS.FSharp.Backend.Quotes

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

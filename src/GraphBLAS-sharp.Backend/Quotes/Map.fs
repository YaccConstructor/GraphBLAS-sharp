module GraphBLAS.FSharp.Backend.Quotes

module Map =
    let optionToValueOrZero<'a> =
        <@ fun (item: 'a option) ->
            match item with
            | Some value -> value
            | None -> Unchecked.defaultof<'a> @>

    let option onSome onNone =
        <@ function
            | Some _ -> onSome
            | None -> onNone @>

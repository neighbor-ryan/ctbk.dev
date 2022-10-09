import {useMemo, useState} from "react";

export type SetSet<T> = {
    add: (t: T) => void
    remove: (t: T) => void
    clear: () => void
}

export function useSet<T>( initialValue: Set<T>): [Set<T>, SetSet<T>] {
    const [set, setSet] = useState(new Set<T>(initialValue));

    const setset: SetSet<T> = useMemo(
        () => ({
            add: (item: T) => setSet(prevSet => {
                console.log("prevSet:", prevSet)
                return new Set([...prevSet, item])
            }),
            remove: (item: T) =>
                setSet(prevSet => new Set([...prevSet].filter(i => i !== item))),
            clear: () => setSet(new Set()),
        }),
        [setSet]
    );

    return [set, setset];
}

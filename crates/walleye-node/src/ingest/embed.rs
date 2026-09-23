//! Which text columns are worth a vector. The model itself is
//! [`walleye_lance::embed::Embedder`].
pub use walleye_lance::embed::Embedder;

/// Whether a text column looks like prose rather than a code, a name or a
/// label: long, and made of words. Only prose is worth a judge's time or an
/// embedding's cost.
pub fn prose(values: &[&str]) -> Prose {
    let seen: Vec<&str> = values
        .iter()
        .copied()
        .filter(|v| !v.trim().is_empty())
        .collect();
    if seen.is_empty() {
        return Prose::No;
    }
    let chars = seen.iter().map(|v| v.chars().count()).sum::<usize>() / seen.len();
    let words = seen
        .iter()
        .map(|v| v.split_whitespace().count())
        .sum::<usize>()
        / seen.len();
    let distinct = seen.iter().collect::<std::collections::BTreeSet<_>>().len();
    if distinct < 2 && seen.len() > 1 {
        return Prose::No;
    }
    if chars >= 60 && words >= 8 {
        Prose::Clearly
    } else if chars >= 24 && words >= 3 {
        Prose::Maybe
    } else {
        Prose::No
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Prose {
    /// Not free text: never embedded.
    No,
    /// Could be; a judge decides, and without one it is not embedded.
    Maybe,
    /// Long sentences. Embedded even without a judge to ask.
    Clearly,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_names_and_labels_are_not_prose() {
        assert_eq!(prose(&["SKU-1234", "SKU-9"]), Prose::No);
        assert_eq!(prose(&["Ada Lovelace", "Grace Hopper"]), Prose::No);
        assert_eq!(prose(&["pending", "shipped", "pending"]), Prose::No);
    }

    #[test]
    fn a_sentence_might_be_and_a_paragraph_is() {
        assert_eq!(
            prose(&["arrived late, box crushed", "fast shipping, happy overall"]),
            Prose::Maybe
        );
        assert_eq!(
            prose(&[
                "The package arrived three days late and the box was crushed on one side.",
                "Great product, fast shipping, and support answered my question within an hour."
            ]),
            Prose::Clearly
        );
    }

    #[test]
    fn the_same_sentence_every_time_is_a_label() {
        let same = "this is the default description text for every item";
        assert_eq!(prose(&[same, same, same]), Prose::No);
    }
}

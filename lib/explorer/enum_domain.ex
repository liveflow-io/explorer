defmodule Explorer.EnumDomain do
  @moduledoc false

  alias Explorer.PolarsBackend.Native

  @enforce_keys [:resource, :size, :fingerprint]
  defstruct [:resource, :size, :fingerprint]

  @type t :: %__MODULE__{
          resource: reference(),
          size: non_neg_integer(),
          fingerprint: non_neg_integer()
        }

  def new(categories), do: Native.enum_domain_new(categories)
  def categories(%__MODULE__{} = domain), do: Native.enum_domain_categories(domain)

  def equal?(
        %__MODULE__{size: size, fingerprint: fingerprint} = left,
        %__MODULE__{size: size, fingerprint: fingerprint} = right
      ),
      do: Native.enum_domain_equal(left, right)

  def equal?(%__MODULE__{}, %__MODULE__{}), do: false
end

defimpl Inspect, for: Explorer.EnumDomain do
  import Inspect.Algebra

  def inspect(domain, _opts) do
    concat([
      "#Explorer.EnumDomain<",
      Integer.to_string(domain.size),
      " categories, ",
      Integer.to_string(domain.fingerprint, 16),
      ">"
    ])
  end
end

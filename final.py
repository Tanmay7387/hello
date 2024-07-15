from mpyc.runtime import mpc
import pandas as pd

async def secure_interaction_count_and_sum(data, amounts):
    """Count interactions and sum amounts securely using MPC."""
    secint = mpc.SecInt()  # Secure integer type for interaction counts
    secfxp = mpc.SecFxp()  # Secure fixed-point type for transaction amounts
    interaction_dict = {}
    sum_dict = {}
    
    for (sender, receiver), amount in zip(data, amounts):
        pair = (str(sender), str(receiver))
        if pair not in interaction_dict:
            interaction_dict[pair] = secint(1)
            sum_dict[pair] = secfxp(amount)
        else:
            interaction_dict[pair] += 1
            sum_dict[pair] += secfxp(amount)

    # Decrypt results securely
    interaction_results = {pair: await mpc.output(count) for pair, count in interaction_dict.items()}
    sum_results = {pair: await mpc.output(sum_amount) for pair, sum_amount in sum_dict.items()}
    return interaction_results, sum_results

async def main():
    await mpc.start()  # Start MPC environment
    
    # Load your dataset
    dataset_path = 'processed_dataset.csv'
    dataset = pd.read_csv(dataset_path)

    # Prepare encrypted sender, receiver data, and transaction amounts for secure computation
    encrypted_interactions = list(zip(dataset['Sender_account'], dataset['Receiver_account']))
    transaction_amounts = dataset['Amount'].tolist()  # Make sure this is the correct column name

    # Compute interaction counts and total transaction amounts securely
    interaction_counts, total_amounts = await secure_interaction_count_and_sum(encrypted_interactions, transaction_amounts)

    # Map interaction counts and total transaction amounts back to the dataset
    dataset['interaction_count'] = dataset.apply(lambda row: interaction_counts.get((str(row['Sender_account']), str(row['Receiver_account'])), 0), axis=1)
    dataset['total_transaction_amount'] = dataset.apply(lambda row: total_amounts.get((str(row['Sender_account']), str(row['Receiver_account'])), 0.0), axis=1)

    # Save the enriched dataset
    dataset.to_csv('enriched_dataset1.csv', index=False)
    print("Dataset has been enriched and saved.")

    await mpc.shutdown()  # Shutdown MPC environment

# Run the MPC computation
mpc.run(main())

